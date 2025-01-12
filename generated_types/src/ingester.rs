use crate::{google::FieldViolation, influxdata::iox::ingester::v1 as proto};
use data_types::timestamp::TimestampRange;
use data_types2::IngesterQueryRequest;
use datafusion::{
    common::DataFusionError, datafusion_proto::bytes::Serializeable, logical_plan::Expr,
};
use predicate::{Predicate, ValueExpr};

fn expr_to_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error converting Expr to bytes: {}", e),
    }
}

fn expr_from_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error creating Expr from bytes: {}", e),
    }
}

impl TryFrom<proto::IngesterQueryRequest> for IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(proto: proto::IngesterQueryRequest) -> Result<Self, Self::Error> {
        let proto::IngesterQueryRequest {
            namespace,
            table,
            columns,
            predicate,
        } = proto;

        let predicate = predicate.map(TryInto::try_into).transpose()?;

        Ok(Self::new(namespace, table, columns, predicate))
    }
}

impl TryFrom<IngesterQueryRequest> for proto::IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(query: IngesterQueryRequest) -> Result<Self, Self::Error> {
        let IngesterQueryRequest {
            namespace,
            table,
            columns,
            predicate,
        } = query;

        Ok(Self {
            namespace,
            table,
            columns,
            predicate: predicate.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<Predicate> for proto::Predicate {
    type Error = FieldViolation;

    fn try_from(pred: Predicate) -> Result<Self, Self::Error> {
        let Predicate {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        } = pred;

        let field_columns = field_columns.into_iter().flatten().collect();
        let range = range.map(|r| proto::TimestampRange {
            start: r.start(),
            end: r.end(),
        });

        let exprs = exprs
            .iter()
            .map(|expr| {
                expr.to_bytes()
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| expr_to_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let value_expr = value_expr
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = FieldViolation;

    fn try_from(proto: proto::Predicate) -> Result<Self, Self::Error> {
        let proto::Predicate {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        } = proto;

        let field_columns = if field_columns.is_empty() {
            None
        } else {
            Some(field_columns.into_iter().collect())
        };

        let range = range.map(|r| TimestampRange::new(r.start, r.end));

        let exprs = exprs
            .into_iter()
            .map(|bytes| {
                Expr::from_bytes_with_registry(&bytes, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let value_expr = value_expr
            .into_iter()
            .map(|ve| {
                let expr = Expr::from_bytes_with_registry(&ve.expr, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("value_expr.expr", e))?;
                // try to convert to ValueExpr
                expr.try_into().map_err(|e| FieldViolation {
                    field: "expr".into(),
                    description: format!("Internal: Serialized expr a valid ValueExpr: {:?}", e),
                })
            })
            .collect::<Result<Vec<ValueExpr>, FieldViolation>>()?;

        Ok(Self {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<ValueExpr> for proto::ValueExpr {
    type Error = FieldViolation;

    fn try_from(value_expr: ValueExpr) -> Result<Self, Self::Error> {
        let expr: Expr = value_expr.into();

        let expr = expr
            .to_bytes()
            .map_err(|e| expr_to_bytes_violation("value_expr.expr", e))?
            .to_vec();

        Ok(Self { expr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn query_round_trip() {
        let rust_predicate = predicate::PredicateBuilder::new()
            .timestamp_range(1, 100)
            .add_expr(col("foo"))
            .add_value_expr(col("_value").eq(lit("bar")).try_into().unwrap())
            .build();

        let rust_query = IngesterQueryRequest::new(
            "mydb".into(),
            "cpu".into(),
            vec!["usage".into(), "time".into()],
            Some(rust_predicate),
        );

        let proto_query: proto::IngesterQueryRequest = rust_query.clone().try_into().unwrap();

        let rust_query_converted: IngesterQueryRequest = proto_query.try_into().unwrap();

        assert_eq!(rust_query, rust_query_converted);
    }
}
