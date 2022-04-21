use generated_types::{
    node::{Comparison, Type as NodeType, Value},
    Node, Predicate,
};

/// Create a predicate representing tag_name=tag_value in the horrible gRPC
/// structs
pub(crate) fn make_tag_predicate(
    tag_name: impl Into<String>,
    tag_value: impl Into<String>,
) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(tag_value.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}

/// Create a predicate representing tag_name ~= /pattern/
pub(crate) fn make_regex_match_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
) -> Predicate {
    make_regex_predicate(tag_key_name, pattern, Comparison::Regex)
}

/// Create a predicate representing tag_name !~ /pattern/
pub(crate) fn make_not_regex_match_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
) -> Predicate {
    make_regex_predicate(tag_key_name, pattern, Comparison::NotRegex)
}

/// Create a predicate representing tag_name <op> /pattern/
///
/// where op is `Regex` or `NotRegEx`
/// The constitution of this request was formed by looking at a real request
/// made to storage, which looked like this:
///
/// root:<
///         node_type:COMPARISON_EXPRESSION
///         children:<node_type:TAG_REF tag_ref_value:"tag_key_name" >
///         children:<node_type:LITERAL regex_value:"pattern" >
///         comparison:REGEX
/// >
pub(crate) fn make_regex_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
    comparison: Comparison,
) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_key_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::RegexValue(pattern.into())),
                },
            ],
            value: Some(Value::Comparison(comparison as _)),
        }),
    }
}
