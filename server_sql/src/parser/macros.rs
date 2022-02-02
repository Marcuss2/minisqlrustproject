macro_rules! value_pattern {
    () => {
        unite!(r"\d+", "NULL", string_pattern!())
    };
}

macro_rules! optional {
  ($($x:expr),+) => {
    concat!("(?:", $($x),+ , ")?")
  };
}

macro_rules! string_pattern {
    () => {
        unite!("\"[^\"]*\"", "'[^']*'")
    };
}

macro_rules! string_or_ident_pattern {
    () => {
        unite!(r"\w+", string_pattern!())
    };
}

macro_rules! value_or_ident_pattern {
    () => {
        unite!(r"\d+", "NULL", string_or_ident_pattern!())
    };
}

macro_rules! value_or_ident_group {
    () => {
        captured!(value_or_ident_pattern!())
    };
}

macro_rules! intersperse {
  ($a:expr; $x:expr) => {
    $x
  };
  ($a:expr; $x:expr, $($y:expr),+) => {
    concat!($x, $a, intersperse!($a; $($y),+))
  }
}

macro_rules! unite {
  ($($x:expr),+) => {
    concat!("(?:", intersperse!("|"; $($x),+), ")")
  };
}

macro_rules! where_pattern {
    () => {
        concat!(
            r"(WHERE)\s+",
            value_or_ident_group!(),
            r"\s*(=|<|>|>=|<=|!=|<>)\s*",
            value_or_ident_group!()
        )
    };
}

macro_rules! commas {
    ($x:expr) => {
        concat!("(?:", $x, s!(), ",", s!(), ")*", $x, optional!(s!(), ","))
    };
}

macro_rules! from_where_pattern {
    () => {
        concat!(
            "(FROM)",
            s1!(),
            captured!(string_or_ident_pattern!()),
            optional!(s1!(), where_pattern!())
        )
    };
}

macro_rules! attr_pattern {
    () => {
        s_delimited!(
            captured!(string_or_ident_pattern!()),
            captured!(type_pattern!(), optional!(s1!(), "PRIMARY", s1!(), "KEY"))
        )
    };
}

macro_rules! command {
  ($($x:expr),+) => {
      concat!(s!(), $($x),+, s!(), ";?", s!())
  };
}

macro_rules! s_delimited {
  ($($x:expr),+) => {
      intersperse!(s1!(); $($x),+)
  };
}

macro_rules! maybe_s_delimited {
  ($($x:expr),+) => {
      intersperse!(s!(); $($x),+)
  };
}

macro_rules! anchored {
    ($x:expr) => {
        concat!("^", $x, r"$")
    };
}

macro_rules! captured {
($($x:expr),+) => {
  concat!("(", $($x),+, ")")
};
}

macro_rules! s {
    () => {
        r"\s*"
    };
}

macro_rules! s1 {
    () => {
        r"\s+"
    };
}

macro_rules! type_pattern {
    () => {
        "(?:INT|INTEGER|STRING|TEXT|VARCHAR|DATA|BLOB)"
    };
}

pub(crate) use {
    anchored, attr_pattern, captured, command, commas, from_where_pattern, intersperse,
    maybe_s_delimited, optional, s, s1, s_delimited, string_or_ident_pattern, string_pattern,
    type_pattern, unite, value_or_ident_group, value_or_ident_pattern, value_pattern,
    where_pattern,
};
