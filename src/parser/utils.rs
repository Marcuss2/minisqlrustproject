use crate::{
    database::{Attribute, AttributeType, Comparison, DataAttribute, DatabaseTable},
    error::UserError,
};
use std::collections::HashMap;

pub fn get_col_to_ix_map(table: &DatabaseTable) -> HashMap<&str, usize> {
    table.attributes.iter().enumerate().map(|(ix, attr)| (attr.name.as_str(), ix)).collect()
}

pub fn parse_selected(cols: &[&str], table: &DatabaseTable) -> Result<Vec<usize>, UserError> {
    if cols == ["*"] {
        Ok((0..table.attributes.len()).collect())
    } else {
        let indices = get_col_to_ix_map(table);
        cols.iter()
            .map(|col| indices.get(col).copied().ok_or(UserError::Other("Column not found")))
            .collect()
    }
}

pub fn parse_values(
    values: &[&str],
    table: &DatabaseTable,
) -> Result<Vec<DataAttribute>, UserError> {
    if values.len() > table.attributes.len() {
        Err(UserError::Other("Too many values"))
    } else {
        values
            .iter()
            .zip(table.attributes.iter())
            .map(|(val, attr)| {
                Ok(match attr.attribute_type {
                    AttributeType::String => DataAttribute::String(parse_string(val)?.to_owned()),
                    AttributeType::Id => DataAttribute::Id(parse_i64(val)?),
                    AttributeType::Number => DataAttribute::Number(parse_i64(val)?),
                    AttributeType::Data => unimplemented!(),
                })
            })
            .collect()
    }
}

pub fn parse_i64(val: &str) -> Result<i64, UserError> {
    val.parse().map_err(|_| UserError::SyntaxError)
}

pub fn parse_comparison(
    lhs: &str,
    cmp: &str,
    rhs: &str,
    table: &DatabaseTable,
) -> Result<(usize, Comparison), UserError> {
    let terms = [lhs, rhs].map(parse_comparison_term);
    let flipped = matches!(terms[0], CmpTerm::Val(_));
    let (attr_pos, data_attr) = match terms {
        [CmpTerm::Ident(col), CmpTerm::Val(val)] | [CmpTerm::Val(val), CmpTerm::Ident(col)] => {
            let (attr_pos, is_pk) = describe_col(col, table)?;
            let val = match val {
                DataAttribute::Number(num) if is_pk => DataAttribute::Id(num),
                x => x,
            };
            (attr_pos, val)
        }
        _ => {
            return Err(UserError::Other(
                "Only single column against literal comparisons supported",
            ))
        }
    };
    let cmp = match (cmp, flipped) {
        (">", false) | ("<", true) => Comparison::Higher(data_attr),
        ("<", false) | (">", true) => Comparison::Lower(data_attr),
        ("=", _) => Comparison::Equal(data_attr),
        (">=", false) | ("<=", true) => todo!(),
        ("<=", false) | (">=", true) => todo!(),
        _ => unreachable!(),
    };
    Ok((attr_pos, cmp))
}

pub fn describe_col(col: &str, table: &DatabaseTable) -> Result<(usize, bool), UserError> {
    table
        .attributes
        .iter()
        .enumerate()
        .find_map(|(ix, attr)| {
            if attr.name == col {
                Some((ix, attr.attribute_type == AttributeType::Id))
            } else {
                None
            }
        })
        .ok_or(UserError::Other("Column not found"))
}

pub enum CmpTerm<'a> {
    Ident(&'a str),
    Val(DataAttribute),
}

pub fn parse_comparison_term<'a>(term: &'a str) -> CmpTerm<'a> {
    if term.starts_with('\'') && term.ends_with('\'') {
        CmpTerm::Val(DataAttribute::String(parse_string(term).unwrap().to_owned()))
    } else if term.starts_with('"') && term.ends_with('"') {
        CmpTerm::Ident(parse_string(term).unwrap())
    } else if let Ok(num) = term.parse::<i64>() {
        CmpTerm::Val(DataAttribute::Number(num))
    } else {
        CmpTerm::Ident(term)
    }
}

pub fn parse_string(val: &str) -> Result<&str, UserError> {
    if (val.starts_with('\'') && val.ends_with('\''))
        || (val.starts_with('"') && val.ends_with('"'))
    {
        Ok(&val[1..val.len() - 1])
    } else {
        Err(UserError::SyntaxError)
    }
}

pub fn parse_attr_type(attr_type: &str) -> Result<AttributeType, UserError> {
    let (type_name, is_pk) = if let Some(ix) = attr_type.find("primary key") {
        (&attr_type[..ix - 1], true)
    } else {
        (attr_type, false)
    };
    match type_name {
        "int" | "integer" => Ok(if is_pk { AttributeType::Id } else { AttributeType::Number }),
        _ if is_pk => Err(UserError::Other("Only integers supported for primary keys")),
        "string" | "varchar" | "text" => Ok(AttributeType::String),
        "data" | "blob" => Ok(AttributeType::Data),
        _ => unreachable!(),
    }
}

pub fn parse_attributes(attrs: &[&str]) -> Result<Vec<Attribute>, UserError> {
    attrs
        .chunks_exact(2)
        .map(|pair| {
            Ok(Attribute { name: pair[0].to_string(), attribute_type: parse_attr_type(pair[1])? })
        })
        .collect()
}
