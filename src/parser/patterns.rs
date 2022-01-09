use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use crate::parser::macros::*;

macro_rules! select_pattern {
    () => {
        s_delimited!(
            "(SELECT)",
            unite!(captured!(r"\*"), commas!(captured!(string_or_ident_pattern!()))),
            from_where_pattern!()
        )
    };
}

macro_rules! delete_pattern {
    () => {
        s_delimited!("(DELETE)", from_where_pattern!())
    };
}

macro_rules! create_table_pattern {
    () => {
        s_delimited!(
            "(CREATE)",
            "(TABLE)",
            captured!(string_or_ident_pattern!()),
            maybe_s_delimited!(r"\(", commas!(attr_pattern!()), r"\)")
        )
    };
}

macro_rules! create_index_pattern {
    () => {
        s_delimited!(
            "(CREATE)",
            "(INDEX)",
            captured!(optional!(string_or_ident_pattern!())),
            "(ON)",
            captured!(string_or_ident_pattern!()),
            maybe_s_delimited!(r"\(", commas!(captured!(string_or_ident_pattern!())), r"\)")
        )
    };
}

macro_rules! insert_pattern {
    () => {
        s_delimited!(
            "(INSERT)",
            "(INTO)",
            captured!(string_or_ident_pattern!()),
            "(VALUES)",
            commas!(captured!(value_pattern!()))
        )
    };
}

macro_rules! drop_pattern {
    () => {
        s_delimited!("(DROP)", "(TABLE)", captured!(string_or_ident_pattern!()))
    };
}

pub fn capture_command(input: &str) -> Vec<&str> {
    lazy_static! {
        static ref RE: Regex = RegexBuilder::new(anchored!(command!(unite!(
            create_table_pattern!(),
            create_index_pattern!(),
            select_pattern!(),
            insert_pattern!(),
            delete_pattern!(),
            drop_pattern!()
        ))))
        .case_insensitive(true)
        .build()
        .unwrap();
    }
    matches_as_vec(input, &RE)
}

fn matches_as_vec<'a>(input: &'a str, re: &Regex) -> Vec<&'a str> {
    if let Some(captures) = re.captures(input) {
        captures.iter().skip(1).flatten().map(|m| m.as_str()).collect()
    } else {
        vec![]
    }
}

fn lowercase_unquoted(input: &str) -> String {
    input
        .chars()
        .scan((None, false), |(last_quote, escaped), c| {
            let result = if last_quote.is_some() { c } else { c.to_ascii_lowercase() };
            if *escaped {
                *escaped = false;
            } else {
                match c {
                    '\\' => *escaped = true,
                    x if Some(x) == *last_quote => *last_quote = None,
                    x @ ('"' | '\'') => *last_quote = Some(x),
                    _ => (),
                };
            };
            Some(result)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_pattern(pattern: &str, input: &str, expected_output: &[&str]) {
        let r = RegexBuilder::new(pattern).case_insensitive(true).build().unwrap();
        let matches = matches_as_vec(input, &r);
        assert_eq!(matches, expected_output);
    }

    #[test]
    fn test_select() {
        assert_pattern(
            select_pattern!(),
            "select a, 'b' from c where d=2",
            &["select", "a", "'b'", "from", "c", "where", "d", "=", "2"],
        )
    }

    #[test]
    fn test_create_table() {
        assert_pattern(
            create_table_pattern!(),
            "create table t ( id int primary key, \"name\" text )",
            &["create", "table", "t", "id", "int primary key", "\"name\"", "text"],
        );
    }

    // #[test]
    // fn test_case_insensitivity() {
    //     assert_pattern(
    //         select_pattern!(),
    //         "SELECT A, 'B' from 'c' Where d=2",
    //         &["select", "a", "'b'", "from", "'c'", "where"],
    //     )
    // }

    #[test]
    fn test_create_index() {
        assert_pattern(
            create_index_pattern!(),
            "create index ab_ix on t (a, b)",
            &["create", "index", "ab_ix", "on", "t", "a", "b"],
        )
    }

    #[test]
    fn test_drop_table() {
        assert_pattern(
            drop_pattern!(),
            "drop table my_table",
            &["drop", "table", "my_table"],
        )
    }
}

// let _ = match matches_as_vec(str, &RE).as_slice() {
//     ["create", "table", name, attrs @ ..] => todo!(),
//     ["create", "index", col_names @ ..] => todo!(),
//     ["select", col_names @ .., "from", table_name] => todo!(),
//     ["select", col_names @ .., "from", table_name, "where", a, cmp, b] => todo!(),
//     ["insert", "into", cols_and_values @ ..] =>
//     ["delete", "from", table_name, "where", a, cmp, b] => todo!(),
//     _ => panic!(),
// };
