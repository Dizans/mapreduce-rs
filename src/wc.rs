use crate::utils::KeyValue;

#[allow(unused_variables)]
pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
    let words: Vec<&str> = contents.split_whitespace().collect();

    let mut kva: Vec<KeyValue> = Vec::new();
    for w in words {
        let kv_obj = KeyValue {
            k: w.to_owned(),
            v: String::from("1"),
        };
        kva.push(kv_obj);
    }
    return kva;
}

#[allow(unused_variables)]
pub fn reduce(key: &str, value: &Vec<String>) -> String {
    return value.len().to_string();
}
