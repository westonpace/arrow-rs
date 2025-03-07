// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines kernel to extract substrings based on a regular
//! expression of a \[Large\]StringArray

use arrow_array::builder::{BooleanBufferBuilder, GenericStringBuilder, ListBuilder};
use arrow_array::*;
use arrow_buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] / [`LargeStringArray`].
/// If `regex_array` element has an empty value, the corresponding result value is always true.
///
/// `flags_array` are optional [`StringArray`] / [`LargeStringArray`] flag, which allow
/// special search modes, such as case insensitive and multi-line mode.
/// See the documentation [here](https://docs.rs/regex/1.5.4/regex/#grouping-and-flags)
/// for more information.
pub fn regexp_is_match_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &GenericStringArray<OffsetSize>,
    flags_array: Option<&GenericStringArray<OffsetSize>>,
) -> Result<BooleanArray, ArrowError> {
    if array.len() != regex_array.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }
    let nulls = NullBuffer::union(array.nulls(), regex_array.nulls());

    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let mut result = BooleanBufferBuilder::new(array.len());

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(
            regex_array
                .iter()
                .zip(flags.iter())
                .map(|(pattern, flags)| {
                    pattern.map(|pattern| match flags {
                        Some(flag) => format!("(?{flag}){pattern}"),
                        None => pattern.to_string(),
                    })
                }),
        ) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };

    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                // Required for Postgres compatibility:
                // SELECT 'foobarbequebaz' ~ ''); = true
                (Some(_), Some(pattern)) if pattern == *"" => {
                    result.append(true);
                }
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re.clone(),
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Regular expression did not compile: {e:?}"
                                ))
                            })?;
                            patterns.insert(pattern, re.clone());
                            re
                        }
                    };
                    result.append(re.is_match(value));
                }
                _ => result.append(false),
            }
            Ok(())
        })
        .collect::<Result<Vec<()>, ArrowError>>()?;

    let data = unsafe {
        ArrayDataBuilder::new(DataType::Boolean)
            .len(array.len())
            .buffers(vec![result.into()])
            .nulls(nulls)
            .build_unchecked()
    };
    Ok(BooleanArray::from(data))
}

/// Perform SQL `array ~ regex_array` operation on [`StringArray`] /
/// [`LargeStringArray`] and a scalar.
///
/// See the documentation on [`regexp_is_match_utf8`] for more details.
pub fn regexp_is_match_utf8_scalar<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex: &str,
    flag: Option<&str>,
) -> Result<BooleanArray, ArrowError> {
    let null_bit_buffer = array.nulls().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(array.len());

    let pattern = match flag {
        Some(flag) => format!("(?{flag}){regex}"),
        None => regex.to_string(),
    };
    if pattern.is_empty() {
        result.append_n(array.len(), true);
    } else {
        let re = Regex::new(pattern.as_str()).map_err(|e| {
            ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
        })?;
        for i in 0..array.len() {
            let value = array.value(i);
            result.append(re.is_match(value));
        }
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };
    Ok(BooleanArray::from(data))
}

/// Extract all groups matched by a regular expression for a given String array.
///
/// Modelled after the Postgres [regexp_match].
///
/// Returns a ListArray of [`GenericStringArray`] with each element containing the leftmost-first
/// match of the corresponding index in `regex_array` to string in `array`
///
/// If there is no match, the list element is NULL.
///
/// If a match is found, and the pattern contains no capturing parenthesized subexpressions,
/// then the list element is a single-element [`GenericStringArray`] containing the substring
/// matching the whole pattern.
///
/// If a match is found, and the pattern contains capturing parenthesized subexpressions, then the
/// list element is a [`GenericStringArray`] whose n'th element is the substring matching
/// the n'th capturing parenthesized subexpression of the pattern.
///
/// The flags parameter is an optional text string containing zero or more single-letter flags
/// that change the function's behavior.
///
/// [regexp_match]: https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
pub fn regexp_match<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &GenericStringArray<OffsetSize>,
    flags_array: Option<&GenericStringArray<OffsetSize>>,
) -> Result<ArrayRef, ArrowError> {
    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::with_capacity(0, 0);
    let mut list_builder = ListBuilder::new(builder);

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(
            regex_array
                .iter()
                .zip(flags.iter())
                .map(|(pattern, flags)| {
                    pattern.map(|pattern| match flags {
                        Some(value) => format!("(?{value}){pattern}"),
                        None => pattern.to_string(),
                    })
                }),
        ) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };

    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                // Required for Postgres compatibility:
                // SELECT regexp_match('foobarbequebaz', ''); = {""}
                (Some(_), Some(pattern)) if pattern == *"" => {
                    list_builder.values().append_value("");
                    list_builder.append(true);
                }
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re.clone(),
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Regular expression did not compile: {e:?}"
                                ))
                            })?;
                            patterns.insert(pattern, re.clone());
                            re
                        }
                    };
                    match re.captures(value) {
                        Some(caps) => {
                            let mut iter = caps.iter();
                            if caps.len() > 1 {
                                iter.next();
                            }
                            for m in iter.flatten() {
                                list_builder.values().append_value(m.as_str());
                            }

                            list_builder.append(true);
                        }
                        None => list_builder.append(false),
                    }
                }
                _ => list_builder.append(false),
            }
            Ok(())
        })
        .collect::<Result<Vec<()>, ArrowError>>()?;
    Ok(Arc::new(list_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ListArray, StringArray};

    #[test]
    fn match_single_group() {
        let values = vec![
            Some("abc-005-def"),
            Some("X-7-5"),
            Some("X545"),
            None,
            Some("foobarbequebaz"),
            Some("foobarbequebaz"),
        ];
        let array = StringArray::from(values);
        let mut pattern_values = vec![r".*-(\d*)-.*"; 4];
        pattern_values.push(r"(bar)(bequ1e)");
        pattern_values.push("");
        let pattern = GenericStringArray::<i32>::from(pattern_values);
        let actual = regexp_match(&array, &pattern, None).unwrap();
        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("005");
        expected_builder.append(true);
        expected_builder.values().append_value("7");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.append(false);
        expected_builder.append(false);
        expected_builder.values().append_value("");
        expected_builder.append(true);
        let expected = expected_builder.finish();
        let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(&expected, result);
    }

    #[test]
    fn match_single_group_with_flags() {
        let values = vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None];
        let array = StringArray::from(values);
        let pattern = StringArray::from(vec![r"x.*-(\d*)-.*"; 4]);
        let flags = StringArray::from(vec!["i"; 4]);
        let actual = regexp_match(&array, &pattern, Some(&flags)).unwrap();
        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::with_capacity(0, 0);
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.append(false);
        expected_builder.values().append_value("7");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.append(false);
        let expected = expected_builder.finish();
        let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(&expected, result);
    }

    #[test]
    fn test_single_group_not_skip_match() {
        let array = StringArray::from(vec![Some("foo"), Some("bar")]);
        let pattern = GenericStringArray::<i32>::from(vec![r"foo"]);
        let actual = regexp_match(&array, &pattern, None).unwrap();
        let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("foo");
        expected_builder.append(true);
        let expected = expected_builder.finish();
        assert_eq!(&expected, result);
    }

    macro_rules! test_flag_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right, None).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let flag = Some(StringArray::from($flag));
                let res = $op(&left, &right, flag.as_ref()).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    macro_rules! test_flag_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right, None).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
        ($test_name:ident, $left:expr, $right:expr, $flag:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let flag = Some($flag);
                let res = $op(&left, $right, flag).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
    }

    test_flag_utf8!(
        test_utf8_array_regexp_is_match,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"],
        vec!["^ar", "^AR", "ow$", "OW$", "foo", ""],
        regexp_is_match_utf8,
        [true, false, true, false, false, true]
    );
    test_flag_utf8!(
        test_utf8_array_regexp_is_match_insensitive,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrow"],
        vec!["^ar", "^AR", "ow$", "OW$", "foo", ""],
        vec!["i"; 6],
        regexp_is_match_utf8,
        [true, true, true, true, false, true]
    );

    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "^ar",
        regexp_is_match_utf8_scalar,
        [true, false, false, false]
    );
    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_empty_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "",
        regexp_is_match_utf8_scalar,
        [true, true, true, true]
    );
    test_flag_utf8_scalar!(
        test_utf8_array_regexp_is_match_insensitive_scalar,
        vec!["arrow", "ARROW", "parquet", "PARQUET"],
        "^ar",
        "i",
        regexp_is_match_utf8_scalar,
        [true, true, false, false]
    );
}
