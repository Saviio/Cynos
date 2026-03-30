use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

#[derive(Clone, Debug)]
pub struct GraphqlResponse {
    pub data: ResponseValue,
}

impl GraphqlResponse {
    pub fn new(data: ResponseValue) -> Self {
        Self { data }
    }
}

impl PartialEq for GraphqlResponse {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

#[derive(Clone, Debug)]
pub enum ResponseValue {
    Null,
    Scalar(Value),
    Object(Rc<[ResponseField]>),
    List(Rc<[ResponseValue]>),
}

impl ResponseValue {
    pub fn object(fields: Vec<ResponseField>) -> Self {
        Self::Object(fields.into())
    }

    pub fn list(items: Vec<ResponseValue>) -> Self {
        Self::List(items.into())
    }
}

impl PartialEq for ResponseValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Scalar(left), Self::Scalar(right)) => left == right,
            (Self::Object(left), Self::Object(right)) => {
                Rc::ptr_eq(left, right) || left.as_ref() == right.as_ref()
            }
            (Self::List(left), Self::List(right)) => {
                Rc::ptr_eq(left, right) || left.as_ref() == right.as_ref()
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResponseField {
    pub name: Rc<str>,
    pub value: ResponseValue,
}

impl ResponseField {
    pub fn new(name: impl Into<String>, value: ResponseValue) -> Self {
        Self {
            name: Rc::<str>::from(name.into()),
            value,
        }
    }
}

impl PartialEq for ResponseField {
    fn eq(&self, other: &Self) -> bool {
        (Rc::ptr_eq(&self.name, &other.name) || self.name.as_ref() == other.name.as_ref())
            && self.value == other.value
    }
}
