use serde::Serializer;

pub fn f64_as_string<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub fn option_f64_as_string<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(f) => {
            let string_value = f.to_string();
            serializer.serialize_some(&string_value)
        }
        None => serializer.serialize_none(),
    }
}
