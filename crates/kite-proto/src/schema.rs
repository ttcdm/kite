use crate::error::{ErrorCode, KiteError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// `name@version`, for example `task.assign@1`. Versions are integers so two
/// agents can never disagree about which revision of a contract they speak.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaRef {
    pub name: String,
    pub version: u32,
}

impl SchemaRef {
    pub fn new(name: impl Into<String>, version: u32) -> Self {
        SchemaRef { name: name.into(), version }
    }
}

impl fmt::Display for SchemaRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.version)
    }
}

impl FromStr for SchemaRef {
    type Err = KiteError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || {
            KiteError::new(ErrorCode::UnknownSchema, format!("schema reference {s:?} is not of the form name@version"))
        };
        let (name, version) = s.rsplit_once('@').ok_or_else(bad)?;
        let name_ok = !name.is_empty()
            && name.len() <= 128
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
        if !name_ok {
            return Err(bad());
        }
        let version: u32 = version.parse().map_err(|_| bad())?;
        if version == 0 {
            return Err(bad());
        }
        Ok(SchemaRef::new(name, version))
    }
}

impl Serialize for SchemaRef {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SchemaRef {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Whether messages of a schema expect an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractKind {
    /// A request; the recipient must answer with the contract's `response` schema.
    Request,
    /// A response to a request. Only valid as some request's `response`.
    Response,
    /// A one-way notification.
    Event,
}

/// One compiled message contract.
pub struct Contract {
    pub schema_ref: SchemaRef,
    pub kind: ContractKind,
    pub response: Option<SchemaRef>,
    pub description: Option<String>,
    pub json_schema: Value,
    validator: jsonschema::Validator,
}

impl fmt::Debug for Contract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Contract")
            .field("schema_ref", &self.schema_ref)
            .field("kind", &self.kind)
            .field("response", &self.response)
            .finish_non_exhaustive()
    }
}

/// One way a payload broke its schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Violation {
    /// JSON Pointer into the payload, `""` for the root.
    pub path: String,
    /// JSON Pointer into the schema keyword that failed.
    pub schema_path: String,
    pub message: String,
}

/// Built-in schema carried by every error envelope.
pub const ERROR_SCHEMA: &str = "kite.error@1";
/// Built-in schema of the event the broker sends a responder when a request it holds is
/// cancelled or times out.
pub const CANCEL_SCHEMA: &str = "kite.cancel@1";

/// The set of contracts a broker enforces. Immutable once built, so it is
/// shared across threads behind an `Arc` with no locking.
#[derive(Debug, Default)]
pub struct Registry {
    contracts: BTreeMap<SchemaRef, Contract>,
}

impl Registry {
    /// A registry holding only the built-in `kite.*` contracts.
    pub fn with_builtins() -> Self {
        let mut r = Registry::default();
        r.insert_unchecked(
            ERROR_SCHEMA.parse().unwrap(),
            ContractKind::Response,
            None,
            Some("Why a request failed. Sent by the broker or by a responder.".into()),
            json!({
                "type": "object",
                "required": ["code", "message"],
                "properties": {
                    "code": {"type": "string", "minLength": 1, "maxLength": 64},
                    "message": {"type": "string", "maxLength": 4096},
                    "detail": {}
                },
                "additionalProperties": false
            }),
        )
        .unwrap();
        r.insert_unchecked(
            CANCEL_SCHEMA.parse().unwrap(),
            ContractKind::Event,
            None,
            Some("A request you received is no longer wanted; stop working on it.".into()),
            json!({
                "type": "object",
                "required": ["request_id", "reason"],
                "properties": {
                    "request_id": {"type": "string"},
                    "reason": {"type": "string", "enum": ["timeout", "cancelled", "peer_down"]}
                },
                "additionalProperties": false
            }),
        )
        .unwrap();
        r.insert_unchecked(
            crate::board::CARD_SCHEMA.parse().unwrap(),
            ContractKind::Event,
            None,
            Some("A card you own or manage changed.".into()),
            json!({
                "type": "object",
                "required": ["card_id", "action", "by", "state", "title"],
                "properties": {
                    "card_id": {"type": "string"},
                    "action": {"type": "string"},
                    "by": {"type": "string"},
                    "state": {"type": "string"},
                    "title": {"type": "string"},
                    "note": {"type": "string"}
                },
                "additionalProperties": false
            }),
        )
        .unwrap();
        r
    }

    /// Adds a user contract. Names under `kite.` are reserved.
    pub fn insert(
        &mut self,
        schema_ref: SchemaRef,
        kind: ContractKind,
        response: Option<SchemaRef>,
        description: Option<String>,
        json_schema: Value,
    ) -> Result<(), String> {
        if schema_ref.name.starts_with("kite.") {
            return Err(format!("{schema_ref}: names starting with \"kite.\" are reserved"));
        }
        if self.contracts.contains_key(&schema_ref) {
            return Err(format!("{schema_ref}: declared twice"));
        }
        self.insert_unchecked(schema_ref, kind, response, description, json_schema)
    }

    fn insert_unchecked(
        &mut self,
        schema_ref: SchemaRef,
        kind: ContractKind,
        response: Option<SchemaRef>,
        description: Option<String>,
        json_schema: Value,
    ) -> Result<(), String> {
        let validator =
            jsonschema::validator_for(&json_schema).map_err(|e| format!("{schema_ref}: invalid JSON Schema: {e}"))?;
        self.contracts
            .insert(schema_ref.clone(), Contract { schema_ref, kind, response, description, json_schema, validator });
        Ok(())
    }

    /// Checks cross-references once every contract is inserted: a request names
    /// an existing response contract, and nothing else names a response.
    pub fn check_links(&self) -> Result<(), String> {
        for c in self.contracts.values() {
            match (c.kind, &c.response) {
                (ContractKind::Request, None) => {
                    return Err(format!("{}: a request must name its response schema", c.schema_ref));
                }
                (ContractKind::Request, Some(r)) => match self.contracts.get(r) {
                    None => return Err(format!("{}: response {r} is not declared", c.schema_ref)),
                    Some(rc) if rc.kind != ContractKind::Response => {
                        return Err(format!("{}: response {r} is not of kind response", c.schema_ref));
                    }
                    Some(_) => {}
                },
                (_, Some(_)) => {
                    return Err(format!("{}: only requests may name a response", c.schema_ref));
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn get(&self, r: &SchemaRef) -> Option<&Contract> {
        self.contracts.get(r)
    }

    pub fn contracts(&self) -> impl Iterator<Item = &Contract> {
        self.contracts.values()
    }

    /// Validates `payload` against the contract `r`, collecting every violation.
    pub fn validate(&self, r: &SchemaRef, payload: &Value) -> Result<(), KiteError> {
        let contract = self
            .get(r)
            .ok_or_else(|| KiteError::new(ErrorCode::UnknownSchema, format!("schema {r} is not declared")))?;
        let violations: Vec<Violation> = contract
            .validator
            .iter_errors(payload)
            .take(32)
            .map(|e| Violation {
                path: e.instance_path().to_string(),
                schema_path: e.schema_path().to_string(),
                message: e.to_string(),
            })
            .collect();
        if violations.is_empty() {
            return Ok(());
        }
        let first = &violations[0];
        let at = if first.path.is_empty() { "the payload root".to_string() } else { first.path.clone() };
        Err(KiteError::new(
            ErrorCode::SchemaViolation,
            format!("payload does not match {r}: at {at}: {}", first.message),
        )
        .with_detail(json!({ "schema": r.to_string(), "violations": violations })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_prints_schema_refs() {
        let r: SchemaRef = "task.assign@3".parse().unwrap();
        assert_eq!(r, SchemaRef::new("task.assign", 3));
        assert_eq!(r.to_string(), "task.assign@3");
        for bad in ["task", "task@", "task@0", "@1", "Task@1", "task@x", "ta sk@1"] {
            assert!(bad.parse::<SchemaRef>().is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn validation_reports_the_failing_path() {
        let mut r = Registry::with_builtins();
        r.insert(
            "t.ev@1".parse().unwrap(),
            ContractKind::Event,
            None,
            None,
            json!({"type":"object","required":["n"],"properties":{"n":{"type":"integer"}}}),
        )
        .unwrap();
        let err = r.validate(&"t.ev@1".parse().unwrap(), &json!({"n": "x"})).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        let v = &err.detail.unwrap()["violations"][0];
        assert_eq!(v["path"], "/n");
        assert!(r.validate(&"t.ev@1".parse().unwrap(), &json!({"n": 2})).is_ok());
    }

    #[test]
    fn request_without_declared_response_is_rejected() {
        let mut r = Registry::with_builtins();
        r.insert("t.req@1".parse().unwrap(), ContractKind::Request, Some("t.res@1".parse().unwrap()), None, json!({}))
            .unwrap();
        assert!(r.check_links().unwrap_err().contains("t.res@1"));
    }

    #[test]
    fn schemas_cannot_reach_the_network_or_other_files() {
        // jsonschema is built without its http and file resolvers, so a remote or file
        // $ref fails at load instead of being fetched.
        for target in ["https://example.com/s.json", "file:///etc/passwd"] {
            let mut r = Registry::with_builtins();
            let e = r.insert("t.ev@1".parse().unwrap(), ContractKind::Event, None, None, json!({"$ref": target}));
            assert!(e.is_err(), "{target} should not resolve");
        }
    }

    #[test]
    fn kite_names_are_reserved() {
        let mut r = Registry::with_builtins();
        let e = r.insert("kite.x@1".parse().unwrap(), ContractKind::Event, None, None, json!({}));
        assert!(e.unwrap_err().contains("reserved"));
    }
}
