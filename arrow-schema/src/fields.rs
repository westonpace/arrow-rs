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

use crate::{ArrowError, Field, FieldRef, SchemaBuilder};
use std::ops::Deref;
use std::sync::Arc;

/// A cheaply cloneable, owned slice of [`FieldRef`]
///
/// Similar to `Arc<Vec<FieldRef>>` or `Arc<[FieldRef]>`
///
/// Can be constructed in a number of ways
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, Fields, SchemaBuilder};
/// // Can be constructed from Vec<Field>
/// Fields::from(vec![Field::new("a", DataType::Boolean, false)]);
/// // Can be constructed from Vec<FieldRef>
/// Fields::from(vec![Arc::new(Field::new("a", DataType::Boolean, false))]);
/// // Can be constructed from an iterator of Field
/// std::iter::once(Field::new("a", DataType::Boolean, false)).collect::<Fields>();
/// // Can be constructed from an iterator of FieldRef
/// std::iter::once(Arc::new(Field::new("a", DataType::Boolean, false))).collect::<Fields>();
/// ```
///
/// See [`SchemaBuilder`] for mutating or updating [`Fields`]
///
/// ```
/// # use arrow_schema::{DataType, Field, SchemaBuilder};
/// let mut builder = SchemaBuilder::new();
/// builder.push(Field::new("a", DataType::Boolean, false));
/// builder.push(Field::new("b", DataType::Boolean, false));
/// let fields = builder.finish().fields;
///
/// let mut builder = SchemaBuilder::from(&fields);
/// builder.remove(0);
/// let new = builder.finish().fields;
/// ```
///
/// [`SchemaBuilder`]: crate::SchemaBuilder
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Fields(Arc<[FieldRef]>);

impl std::fmt::Debug for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl Fields {
    /// Returns a new empty [`Fields`]
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }

    /// Return size of this instance in bytes.
    pub fn size(&self) -> usize {
        self.iter()
            .map(|field| field.size() + std::mem::size_of::<FieldRef>())
            .sum()
    }

    /// Searches for a field by name, returning it along with its index if found
    pub fn find(&self, name: &str) -> Option<(usize, &FieldRef)> {
        self.0.iter().enumerate().find(|(_, b)| b.name() == name)
    }

    /// Check to see if `self` is a superset of `other`
    ///
    /// In particular returns true if both have the same number of fields, and [`Field::contains`]
    /// for each field across self and other
    ///
    /// In other words, any record that conforms to `other` should also conform to `self`
    pub fn contains(&self, other: &Fields) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b) || a.contains(b))
    }

    /// Remove a field by index and return it.
    ///
    /// # Panic
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Example
    /// ```
    /// use arrow_schema::{DataType, Field, Fields};
    /// let mut fields = Fields::from(vec![
    ///   Field::new("a", DataType::Boolean, false),
    ///   Field::new("b", DataType::Int8, false),
    ///   Field::new("c", DataType::Utf8, false),
    /// ]);
    /// assert_eq!(fields.len(), 3);
    /// assert_eq!(fields.remove(1), Field::new("b", DataType::Int8, false).into());
    /// assert_eq!(fields.len(), 2);
    /// ```
    pub fn remove(&mut self, index: usize) -> FieldRef {
        let mut builder = SchemaBuilder::from(Fields::from(&*self.0));
        let field = builder.remove(index);
        *self = builder.finish().fields;
        field
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[FieldRef]> for Fields {
    fn from(value: &[FieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[FieldRef; N]> for Fields {
    fn from(value: [FieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A cheaply cloneable, owned collection of [`FieldRef`] and their corresponding type ids
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct UnionFields(Arc<[(i8, FieldRef)]>);

impl std::fmt::Debug for UnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl UnionFields {
    /// Create a new [`UnionFields`] with no fields
    pub fn empty() -> Self {
        Self(Arc::from([]))
    }

    /// Create a new [`UnionFields`] from a [`Fields`] and array of type_ids
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with type id mapping
    /// // 1 -> DataType::UInt8
    /// // 3 -> DataType::Utf8
    /// UnionFields::new(
    ///     vec![1, 3],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field3", DataType::Utf8, false),
    ///     ],
    /// );
    /// ```
    pub fn new<F, T>(type_ids: T, fields: F) -> Self
    where
        F: IntoIterator,
        F::Item: Into<FieldRef>,
        T: IntoIterator<Item = i8>,
    {
        let fields = fields.into_iter().map(Into::into);
        let mut set = 0_u128;
        type_ids
            .into_iter()
            .map(|idx| {
                let mask = 1_u128 << idx;
                if (set & mask) != 0 {
                    panic!("duplicate type id: {}", idx);
                } else {
                    set |= mask;
                }
                idx
            })
            .zip(fields)
            .collect()
    }

    /// Return size of this instance in bytes.
    pub fn size(&self) -> usize {
        self.iter()
            .map(|(_, field)| field.size() + std::mem::size_of::<(i8, FieldRef)>())
            .sum()
    }

    /// Returns the number of fields in this [`UnionFields`]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if this is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over the fields and type ids in this [`UnionFields`]
    ///
    /// Note: the iteration order is not guaranteed
    pub fn iter(&self) -> impl Iterator<Item = (i8, &FieldRef)> + '_ {
        self.0.iter().map(|(id, f)| (*id, f))
    }

    /// Merge this field into self if it is compatible.
    ///
    /// See [`Field::try_merge`]
    pub(crate) fn try_merge(&mut self, other: &Self) -> Result<(), ArrowError> {
        // TODO: This currently may produce duplicate type IDs (#3982)
        let mut output: Vec<_> = self.iter().map(|(id, f)| (id, f.clone())).collect();
        for (field_type_id, from_field) in other.iter() {
            let mut is_new_field = true;
            for (self_type_id, self_field) in output.iter_mut() {
                if from_field == self_field {
                    // If the nested fields in two unions are the same, they must have same
                    // type id.
                    if *self_type_id != field_type_id {
                        return Err(ArrowError::SchemaError(
                            format!("Fail to merge schema field '{}' because the self_type_id = {} does not equal field_type_id = {}",
                                    self_field.name(), self_type_id, field_type_id)
                        ));
                    }

                    is_new_field = false;
                    break;
                }
            }

            if is_new_field {
                output.push((field_type_id, from_field.clone()))
            }
        }
        *self = output.into_iter().collect();
        Ok(())
    }
}

impl FromIterator<(i8, FieldRef)> for UnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, FieldRef)>>(iter: T) -> Self {
        // TODO: Should this validate type IDs are unique (#3982)
        Self(iter.into_iter().collect())
    }
}
