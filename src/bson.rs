#[link(name = "bson",
       vers = "0.1",
       uuid = "812ef35d-c3f5-4d94-9199-a2346bfa346e")];
#[crate_type = "lib"];
extern mod std;

use core::str::from_bytes;
use std::treemap::TreeMap;
// use

use core::io::{WriterUtil,ReaderUtil};

// Available Result values
enum Result {
  Document(~BsonElement),
  ParseError(bool)
}

// BsonTypes
enum BsonTypes {
  BsonDouble = 0x01,
  BsonString = 0x02,
  BsonObject = 0x03,
  BsonArray = 0x04,
  BsonBinary = 0x05,
  BsonUndefined = 0x06,
  BsonObjectId = 0x07,
  BsonBoolean = 0x08,
  BsonDate = 0x09,
  BsonNull = 0x0a,
  BsonRegexp = 0x0b,
  BsonJavascriptCode = 0x0d,
  BsonSymbol = 0x0e,
  BsonJavascriptCodeWScope = 0x0f,  
  BsonInt32 = 0x10,
  BsonTimestamp = 0x11,
  BsonInt64 = 0x12,
  BsonMinKey = 0xff,
  BsonMaxKey = 0x7f
}

// Available Bson Element types
enum BsonElement {
  Double(f64),
  String(@str),
  Object(@mut TreeMap<~str, @BsonElement>),
  Array(@[@BsonElement]),  
  Binary(@[u8], u8),
  Undefined,
  ObjectId(@[u8]),
  Boolean(bool),
  DateTime(u64),
  Null,
  RegExp(@str, @str),
  JavascriptCode(@str),
  Symbol(@str),
  JavascriptCodeWScope(@str, @BsonElement),  
  Int32(i32),
  Timestamp(u64),
  Int64(i64),  
  MinKey,
  MaxKey
}

/*
 * Utility methods
 */
priv fn to_double(v:u64) -> f64 {unsafe { cast::transmute(v) }}

/*
 * BSON Decoder
 */
pub struct Decoder {
  reader: @io::Reader,
  next_byte: @mut Option<u8>
}

pub impl Decoder {
  fn new(reader: @io::Reader) -> Decoder {
    Decoder {
      reader: reader,
      next_byte: @mut None,
    }
  }

  priv fn parse_object(&self) ->@BsonElement {
    // Read object size
    self.reader.read_le_u32();
    // Store all the values of the object in order
    let map = @mut TreeMap::new::<~str, @BsonElement>();

    // Loop over all items in the object
    loop {
      // Read the object type
      let bson_type:u8 = self.reader.read_u8();
      // If bson_type == 0 we are done
      if bson_type == 0 {
        break;
      }
      
      // Decode the name from the cstring
      let name = self.reader.read_c_str();
      
      // Match on the type
      match bson_type {
        0x01 => { map.insert(name, self.parse_double()); },
        0x02 => { map.insert(name, self.parse_string()); },
        0x03 => { map.insert(name, self.parse_object()); },
        0x04 => { map.insert(name, self.parse_array()); }, 
        0x05 => { map.insert(name, self.parse_binary()); },
        0x06 => { map.insert(name, @Undefined); },
        0x07 => { map.insert(name, @ObjectId(at_vec::from_owned(self.reader.read_bytes(12)))); },
        0x08 => { map.insert(name, self.parse_bool()); },
        0x09 => { map.insert(name, @DateTime(self.reader.read_le_u64())); },
        0x0a => { map.insert(name, @Null); },
        0x0b => { map.insert(name, self.parse_regexp()); },
        0x0d => { map.insert(name, self.parse_javascript()); },
        0x0e => { map.insert(name, self.parse_symbol()); },
        0x0f => { map.insert(name, self.parse_javascript_w_scope()); },
        0x10 => { map.insert(name, @Int32(self.reader.read_le_i32())); },
        0x11 => { map.insert(name, @Timestamp(self.reader.read_le_u64())); },
        0x12 => { map.insert(name, @Int64(self.reader.read_le_i64())); },
        0xff => { map.insert(name, @MinKey); },
        0x7f => { map.insert(name, @MaxKey); },
        _ => fail!(~"Invalid bson type")
      }      
    }

    @Object(map)
  }

  #[inline(always)]
  priv fn parse_array(&self) -> @BsonElement {
    let object = self.parse_object();
    match object {
      @Object(map) => {
        let vector = do at_vec::build |push| {
          for map.each_value |value| {
            push(*value);
          }
        };

        @Array(vector)
      },
      _ => fail!()
    }
  }

  #[inline(always)]
  priv fn parse_double(&self) -> @BsonElement {
    // Read u64 value
    let u64_value = self.reader.read_le_u64();
    // Cast to f64
    let value = to_double(u64_value);
    // Insert value
    @Double(value)
  }

  #[inline(always)]
  priv fn parse_string(&self) -> @BsonElement {
    let string_size = self.reader.read_le_u32() - 1;
    let bytes = self.reader.read_bytes(string_size as uint);
    // Skip zero
    self.reader.read_u8();
    // Convert bytes
    let string = from_bytes(bytes);
    // Return the value
    @String(string.to_managed())
  }

  #[inline(always)]
  priv fn parse_binary(&self) -> @BsonElement {
    let binary_size = self.reader.read_le_u32();
    let sub_type = self.reader.read_u8();
    let bytes = at_vec::from_owned(self.reader.read_bytes(binary_size as uint));
    @Binary(bytes, sub_type)
  }

  #[inline(always)]
  priv fn parse_bool(&self) -> @BsonElement {
    let boolValue = self.reader.read_u8();
    if boolValue == 0 {
      @Boolean(false)
    } else {
      @Boolean(true)
    }
  }

  #[inline(always)]
  priv fn parse_regexp(&self) -> @BsonElement {
    let reg_exp = self.reader.read_c_str();
    let options = self.reader.read_c_str();
    @RegExp(reg_exp.to_managed(), options.to_managed()) 
  }

  #[inline(always)]
  priv fn parse_symbol(&self) -> @BsonElement {
    let string_size = self.reader.read_le_u32() - 1;
    let bytes = self.reader.read_bytes(string_size as uint);
    // Skip zero
    self.reader.read_u8();
    // Convert bytes
    @Symbol(from_bytes(bytes).to_managed())
  }

  #[inline(always)]
  priv fn parse_javascript_w_scope(&self) -> @BsonElement {
    // Skip the first 4 bytes
    self.reader.read_le_u32();
    // Read string size
    let string_size = self.reader.read_le_u32() - 1;
    let bytes = self.reader.read_bytes(string_size as uint);    
    // Skip zero
    self.reader.read_u8();
    // Parse the document
    let document = self.parse_object();
    // Convert bytes
    @JavascriptCodeWScope(from_bytes(bytes).to_managed(), document) 
  }

  #[inline(always)]
  priv fn parse_javascript(&self) -> @BsonElement {
    let string_size = self.reader.read_le_u32() - 1;
    let bytes = self.reader.read_bytes(string_size as uint);
    // Skip zero
    self.reader.read_u8();
    // Convert bytes
    @JavascriptCode(from_bytes(bytes).to_managed())
  }

  fn parse(&self) -> @BsonElement {
    self.parse_object()
  }
}

// #[test]
// fn parse_simple_int32() {
//   let data = @[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
//   io::with_bytes_reader(data, |rd| {
//     let decoder = Decoder::new(rd);  
//     let obj = decoder.parse();
//     io::println(fmt!("%?", obj));
//   });
// }

#[test]
fn deserialize_full_document() {
  let data = @[69,1,0,0,255,109,105,110,107,101,121,0,127,109,97,120,107,101,121,0,2,115,116,114,105,110,103,0,6,0,0,0,104,101,108,108,111,0,4,97,114,114,97,121,0,26,0,0,0,16,48,0,1,0,0,0,16,49,0,2,0,0,0,16,50,0,3,0,0,0,0,3,104,97,115,104,0,19,0,0,0,16,97,0,1,0,0,0,16,98,0,2,0,0,0,0,9,100,97,116,101,0,181,137,245,137,62,1,0,0,7,111,105,100,0,81,139,195,250,222,146,140,112,39,0,0,1,5,98,105,110,97,114,121,0,5,0,0,0,0,104,101,108,108,111,16,105,110,116,0,42,0,0,0,1,102,108,111,97,116,0,223,224,11,147,169,170,64,64,11,114,101,103,101,120,112,0,114,101,103,101,120,112,0,0,8,98,111,111,108,101,97,110,0,1,18,108,111,110,103,0,100,0,0,0,0,0,0,0,3,100,98,114,101,102,0,70,0,0,0,2,36,114,101,102,0,10,0,0,0,110,97,109,101,115,112,97,99,101,0,7,36,105,100,0,81,139,195,250,222,146,140,112,39,0,0,2,2,36,100,98,0,19,0,0,0,105,110,116,101,103,114,97,116,105,111,110,95,116,101,115,116,115,95,0,0,15,119,104,101,114,101,0,31,0,0,0,11,0,0,0,116,104,105,115,46,97,32,62,32,105,0,12,0,0,0,16,105,0,1,0,0,0,0,0];
  io::with_bytes_reader(data, |rd| {
    let decoder = Decoder::new(rd);  
    let obj = decoder.parse();

    match obj {
      @Object(map) => {
        match map.find(&~"maxkey") {
          Some(&@MaxKey) => (),
          _ => fail!()
        }

        match map.find(&~"minkey") {
          Some(&@MinKey) => (),
          _ => fail!()
        }

        match map.find(&~"long") {
          Some(&@Int64(number)) => assert_eq!(number, 100 as i64),
          _ => fail!()
        }

        match map.find(&~"boolean") {
          Some(&@Boolean(value)) => assert_eq!(value, true),
          _ => fail!()
        }

        match map.find(&~"binary") {
          Some(&@Binary(data1, subtype1)) => {
            // io::println(fmt!("%?", str::from_bytes(data1)));
            assert_eq!(str::from_bytes(data1), ~"hello");
            assert_eq!(subtype1, 0);
            ()
          },
          _ => fail!()
        }

        match map.find(&~"oid") {
          Some(&@ObjectId(oid)) => assert_eq!(oid.len(), 12),
          _ => fail!()
        }

        match map.find(&~"array") {
          Some(&@Array(array)) => {
            match array[0] {
              @Int32(number) => assert_eq!(number, 1),
              _ => fail!()
            }

            match array[1] {
              @Int32(number) => assert_eq!(number, 2),
              _ => fail!()
            }

            match array[2] {
              @Int32(number) => assert_eq!(number, 3),
              _ => fail!()
            }
          }
          _ => fail!()
        }

        match map.find(&~"hash") {
          Some(&@Object(map2)) => {
            match map2.find(&~"a") {
              Some(&@Int32(number)) => assert_eq!(number, 1),
              _ => fail!()
            }

            match map2.find(&~"b") {
              Some(&@Int32(number)) => assert_eq!(number, 2),
              _ => fail!()
            }
          }
          _ => fail!()
        }

        match map.find(&~"date") {
          Some(&@DateTime(_)) => (),
          _ => fail!()          
        }

        match map.find(&~"string") {
          Some(&@String(string)) => assert_eq!(string, @"hello"),
          _ => fail!()
        }

        match map.find(&~"int") {
          Some(&@Int32(number)) => assert_eq!(number, 42),
          _ => fail!()
        }

        match map.find(&~"float") {
          Some(&@Double(number)) => assert_eq!(number, 33.3333),
          _ => fail!()
        }

        match map.find(&~"regexp") {
          Some(&@RegExp(regexp, _)) => assert_eq!(regexp, @"regexp"),
          _ => fail!()
        }

        match map.find(&~"where") {
          Some(&@JavascriptCodeWScope(code, document)) => {
            match document {
              @Object(map1) => {
                match map1.find(&~"i") {
                  Some(&@Int32(number)) => assert_eq!(number, 1),
                  _ => fail!()                  
                }
              },
              _ => fail!()
            }

            assert_eq!(code, @"this.a > i");
          },
          _ => fail!()
        }
      },
      _ => fail!()
    }
  });
}



// struct BsonParser;

// impl BsonParser {
//   fn serialize_object(&self, object: &BsonElement, data: &mut [u8], index: &mut uint) {
//     // Unpack the object
//     match object {
//       &Object(map) => {
//         // Get each key
//         for map.each_key |k| {
//           // Let's figure out what type of object we have
//           match map.find(k) {
//             Some(&~Int32(number)) => {
//               // Set the data type
//               data[*index] = BsonInt32 as u8;
//               // Adjust index
//               *index += 1;

//               // Copy the field value name to the vector
//               copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

//               // Adjust the index position
//               *index += k.len() + 1;

//               // Write the int32 value to the data
//               data[*index + 3] = ((number >> 24) & 0xff) as u8;
//               data[*index + 2] = ((number >> 16) & 0xff) as u8;
//               data[*index + 1] = ((number >> 8) & 0xff) as u8;
//               data[*index] = (number & 0xff) as u8;
//               *index += 4;
//             },
//             Some(&~Int64(number)) => {
//               // Set the data type
//               data[*index] = BsonInt64 as u8;
//               // Adjust index
//               *index += 1;

//               // Copy the field value name to the vector
//               copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

//               // Adjust the index position
//               *index += k.len() + 1;

//               // Save the int64 number
//               data[*index + 7] = ((number >> 56) & 0xff) as u8;
//               data[*index + 6] = ((number >> 48) & 0xff) as u8;
//               data[*index + 5] = ((number >> 40) & 0xff) as u8;
//               data[*index + 4] = ((number >> 32) & 0xff) as u8;
//               data[*index + 3] = ((number >> 24) & 0xff) as u8;
//               data[*index + 2] = ((number >> 16) & 0xff) as u8;
//               data[*index + 1] = ((number >> 8) & 0xff) as u8;
//               data[*index] = (number & 0xff) as u8;
//               *index += 8;
//             },
//             Some(&~Object(map)) => {
//               // Set type
//               data[*index] = BsonObject as u8;
//               // Skip type
//               *index += 1;
//               // Copy the field value name to the vector
//               copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

//               // Adjust the index position
//               *index += k.len() + 1;

//               // Save position for size calculation
//               let starting_index = *index;
//               // Skip to start of doc
//               *index += 4;

//               // Serialize the object
//               self.serialize_object(~Object(map), data, index);

//               // Calculate size
//               let size = *index - starting_index + 1;
//               // Write the size of the document
//               data[starting_index + 3] = ((size >> 24) & 0xff) as u8;
//               data[starting_index + 2] = ((size >> 16) & 0xff) as u8;
//               data[starting_index + 1] = ((size >> 8) & 0xff) as u8;
//               data[starting_index] = (size & 0xff) as u8;
//               // Adjust past the last 0
//               if *index < data.len() {                
//                 *index += 1;
//               }
//             }
//             _ => ()
//           }
//         }
//       }
//       _ => ()
//     }
//   }

//   fn serialize(&self, object: &BsonElement) -> ~[u8] {
//     // Calculate size of final object
//     let size = BsonParser::calculateSize(object);
    
//     // Allocate a vector
//     let mut data = vec::from_elem(size, 0);

//     // Write the data to the vector
//     data[3] = ((size >> 24) & 0xff) as u8;
//     data[2] = ((size >> 16) & 0xff) as u8;
//     data[1] = ((size >> 8) & 0xff) as u8;
//     data[0] = (size & 0xff) as u8;

//     // Starting index
//     let mut index = @mut 4;

//     // Serialize the object
//     self.serialize_object(object, data, index);

//     // data
//     data
//   }

//   fn calculateSize(object: &BsonElement) -> uint {
//     let mut size:uint = 0;

//     // Unpack the object
//     match object {
//       &Object(map) => {
//         // Add the header and tail of the document
//         size += 5;
//         // Iterate over all the fields
//         for map.each_key |k| {
//           // String length + 0 terminating byte + type
//           size += k.len() as uint + 1 + 1;
//           // Match the key
//           size += match map.find(k) {
//             Some(&~Int32(_)) => 4,
//             Some(&~Object(map)) => BsonParser::calculateSize(~Object(map)),
//             Some(&~Int64(_)) => 8,
//             _ => 0
//           };
//         };
//       },
//       _ => ()
//     }

//     size
//   }