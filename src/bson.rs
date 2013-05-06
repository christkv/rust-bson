extern mod std;

use core::str::raw::from_c_str;
use core::str::from_bytes;
use core::vec::const_slice;
use core::vec::slice;
use core::cast::transmute;
use core::vec::bytes::copy_memory;
use std::treemap::TreeMap;
use std::dlist::DList;
// use core::dlist::*;

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
  Object(@mut TreeMap<~str, ~BsonElement>),
  Array(@mut std::dlist::DList<~BsonElement>),  
  Binary(~[u8], u8),
  Undefined,
  ObjectId(~[u8]),
  Boolean(bool),
  DateTime(u64),
  Null,
  RegExp(~str, ~str),
  JavascriptCode(@str),
  Symbol(@str),
  JavascriptCodeWScope(@str, ~BsonElement),  
  Int32(i32),
  Timestamp(u64),
  Int64(i64),  
  MinKey,
  MaxKey
}

struct BsonParser;
struct ParserState {
  index: u32,
  document: @BsonElement
}

impl BsonParser {
  fn serialize_object(&self, object: &BsonElement, data: &mut [u8], index: &mut uint) {
    // Unpack the object
    match object {
      &Object(map) => {
        // Get each key
        for map.each_key |k| {
          // Let's figure out what type of object we have
          match map.find(k) {
            Some(&~Int32(number)) => {
              // Set the data type
              data[*index] = BsonInt32 as u8;
              // Adjust index
              *index += 1;

              // Copy the field value name to the vector
              copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

              // Adjust the index position
              *index += k.len() + 1;

              // Write the int32 value to the data
              data[*index + 3] = ((number >> 24) & 0xff) as u8;
              data[*index + 2] = ((number >> 16) & 0xff) as u8;
              data[*index + 1] = ((number >> 8) & 0xff) as u8;
              data[*index] = (number & 0xff) as u8;
              *index += 4;
            },
            Some(&~Int64(number)) => {
              // Set the data type
              data[*index] = BsonInt64 as u8;
              // Adjust index
              *index += 1;

              // Copy the field value name to the vector
              copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

              // Adjust the index position
              *index += k.len() + 1;

              // Save the int64 number
              data[*index + 7] = ((number >> 56) & 0xff) as u8;
              data[*index + 6] = ((number >> 48) & 0xff) as u8;
              data[*index + 5] = ((number >> 40) & 0xff) as u8;
              data[*index + 4] = ((number >> 32) & 0xff) as u8;
              data[*index + 3] = ((number >> 24) & 0xff) as u8;
              data[*index + 2] = ((number >> 16) & 0xff) as u8;
              data[*index + 1] = ((number >> 8) & 0xff) as u8;
              data[*index] = (number & 0xff) as u8;
              *index += 8;
            },
            Some(&~Object(map)) => {
              // Set type
              data[*index] = BsonObject as u8;
              // Skip type
              *index += 1;
              // Copy the field value name to the vector
              copy_memory(vec::mut_slice(data, *index, *index + k.len()), k.to_bytes(), k.len());

              // Adjust the index position
              *index += k.len() + 1;

              // Save position for size calculation
              let starting_index = *index;
              // Skip to start of doc
              *index += 4;

              // Serialize the object
              self.serialize_object(~Object(map), data, index);

              // Calculate size
              let size = *index - starting_index + 1;
              // Write the size of the document
              data[starting_index + 3] = ((size >> 24) & 0xff) as u8;
              data[starting_index + 2] = ((size >> 16) & 0xff) as u8;
              data[starting_index + 1] = ((size >> 8) & 0xff) as u8;
              data[starting_index] = (size & 0xff) as u8;
              // Adjust past the last 0
              if *index < data.len() {                
                *index += 1;
              }
            }
            _ => ()
          }
        }
      }
      _ => ()
    }
  }

  fn serialize(&self, object: &BsonElement) -> ~[u8] {
    // Calculate size of final object
    let size = BsonParser::calculateSize(object);
    
    // Allocate a vector
    let mut data = vec::from_elem(size, 0);

    // Write the data to the vector
    data[3] = ((size >> 24) & 0xff) as u8;
    data[2] = ((size >> 16) & 0xff) as u8;
    data[1] = ((size >> 8) & 0xff) as u8;
    data[0] = (size & 0xff) as u8;

    // Starting index
    let mut index = @mut 4;

    // Serialize the object
    self.serialize_object(object, data, index);

    // data
    data
  }

  fn calculateSize(object: &BsonElement) -> uint {
    let mut size:uint = 0;

    // Unpack the object
    match object {
      &Object(map) => {
        // Add the header and tail of the document
        size += 5;
        // Iterate over all the fields
        for map.each_key |k| {
          // String length + 0 terminating byte + type
          size += k.len() as uint + 1 + 1;
          // Match the key
          size += match map.find(k) {
            Some(&~Int32(_)) => 4,
            Some(&~Object(map)) => BsonParser::calculateSize(~Object(map)),
            Some(&~Int64(_)) => 8,
            _ => 0
          };
        };
      },
      _ => ()
    }

    size
  }

  fn deserialize(&self, data: &[u8]) -> @Result {
    // Get the initial state of the parsing
    let size = data[0] as u32;
    // Return an error if we the sizes of the message are not the same
    if size != data.len() as u32 {
      // return @ParseError {error: true} as @Result;
      return @ParseError(true);
    }

    // If we have zero elements (special case)
    if(size == 5) {
      return @Document(~Object(@mut TreeMap::new::<~str, ~BsonElement>()));
    }

    // Parse the document
    let object = BsonParser::deserialize_loop(data, @mut 0i64);
    io::println("+++++++++++++++++++++++++++++ deserialize");
    // return the document
    @Document(object)
  }

  fn deserialize_loop(data: &[u8], index: &mut i64) -> ~BsonElement {  
    io::println("+++++++++++++++++++++++++++++ deserialize_object");
    // Create an empty object
    let map = @mut TreeMap::new::<~str, ~BsonElement>();
    let object: ~BsonElement = ~Object(map);
    // Adjust the location of the index
    *index = *index + 4;

    // Loop until we are done
    loop {
      // Get bson type
      let bson_type = data[*index];
      // Adjust to name of the field
      *index += 1;
      // If type is 0x00 we are done
      if bson_type == 0x00 {
        break;        
      }

      io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? :: %?", bson_type, *index));

      // Decode the name from the cstring
      let name = BsonParser::extract_string(*index, data); 
      // Adjust the index to point to the data
      *index += name.len() as i64 + 1;

      // Match bson type
      match bson_type as u8 {
        0x02 => {
            map.insert(name, BsonParser::parseString(index, data));
          },
        0x03 => {
            map.insert(name, BsonParser::deserialize_loop(data, index));
          }
        0x04 => {
            map.insert(name, BsonParser::deserialize_array(data, index));
          },
        0x10 => {
            map.insert(name, BsonParser::parseInt32(index, data));
          },
        0x12 => {
            map.insert(name, BsonParser::parseInt64(index, data));
          },
        _ => ()
      }

      io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? :: %? ---", bson_type, *index));

    }

      io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? --- end", *index));
    object
  }

  fn deserialize_array(data: &[u8], index: &mut i64) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ deserialize_array");
    // Create an empty object
    let list = DList::<~BsonElement>();
    let array = ~Array(list);

    // Adjust the location of the index
    *index = *index + 4;
    // Loop until we are done
    loop {
      // Get bson type
      let bson_type = data[*index];
      io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_array %? :: %?", bson_type, *index));
      // Adjust to name of the field
      *index += 1;
      // If type is 0x00 we are done
      if bson_type == 0x00 {
        break;        
      }

      // Decode the name from the cstring
      let name = BsonParser::extract_string(*index, data); 
      // Adjust the index to point to the data
      *index += name.len() as i64 + 1;

      // Match bson type
      match bson_type as u8 {
        0x01 => {
            list.push(BsonParser::parseDouble(index, data));
          },
        0x02 => {
            list.push(BsonParser::parseString(index, data));
          },
        0x03 => {
            list.push(BsonParser::deserialize_loop(data, index));
          }
        0x04 => {
            list.push(BsonParser::deserialize_array(data, index));
          },
        0x05 => {
            list.push(BsonParser::parseBinary(index, data));
          },
        0x06 => {
            list.push(~Undefined);
          },
        0x07 => {
            list.push(BsonParser::parseObjectId(index, data));
          },
        0x08 => {
            list.push(BsonParser::parseBoolean(index, data));
          },
        0x09 => {
            list.push(BsonParser::parseDateTime(index, data));
          },
        0x0a => {
            list.push(~Null);
          },
        0x0b => {
            list.push(BsonParser::parseRegExp(index, data));
          },
        0x0d => {
            list.push(BsonParser::parseJavaScriptCode(index, data));
          },
        0x0e => {
            list.push(BsonParser::parseSymbol(index, data));
          },
        0x0f => {
            list.push(BsonParser::parseJavaScriptCodeWScope(index, data));
          },
        0x10 => {
            list.push(BsonParser::parseInt32(index, data));
          },
        0x11 => {
            list.push(BsonParser::parseTimestamp(index, data));
          },
        0x12 => {
            list.push(BsonParser::parseInt64(index, data));
          },
        0xff => {
            list.push(~MinKey);
          },
        0x7f => {
            list.push(~MaxKey);
          },
        _ => ()
      }      
    }

    // return array;
    array
  }

  fn extract_string(index:i64, data: &[u8]) -> ~str {
    unsafe {
      let data2: &[i8] = transmute(data);
      // Unpack the name of the field
      from_c_str(&data2[index])
    }
  }

  fn parseTimestamp(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseTimestamp");
    // Unpack the i32 value
    let value = ~Timestamp(data[*index] as u64);
    // Adjust index
    *index += 8;
    // Return the value
    value
  }

  fn parseSymbol(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseSymbol");
    // unpack the string size
    let size:u32 = data[*index] as u32;
    // Adjust the index
    *index = *index + 4;
    // unpack the data as a string
    let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
    // Adjust the index
    *index = *index + (size as i64);
    // return string
    ~Symbol(string)
  }

  fn parseJavaScriptCodeWScope(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseJavaScriptCodeWScope");
    // unpack the string size
    let size:u32 = data[*index] as u32;
    // Adjust the index
    *index = *index + 4;
    // unpack the data as a string
    let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
    // Adjust the index
    *index = *index + (size as i64);
    // Parse the document
    let document = BsonParser::deserialize_loop(data, index);
    // Return the value
    ~JavascriptCodeWScope(string, document)
  }

  fn parseJavaScriptCode(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseJavaScriptCode");
    // unpack the string size
    let size:u32 = data[*index] as u32;
    // Adjust the index
    *index = *index + 4;
    // unpack the data as a string
    let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
    // Adjust the index
    *index = *index + (size as i64);
    // return string
    ~JavascriptCode(string)
  }

  fn parseRegExp(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseRegExp");
    // Decode the regexp from the cstring
    let reg_exp = BsonParser::extract_string(*index, data); 
    // Adjust the index to point to the data
    *index += reg_exp.len() as i64 + 1;
    // Decode the options
    let options_exp = BsonParser::extract_string(*index, data);
    // Adjust the index to point to the data
    *index += options_exp.len() as i64 + 1;
    // Return the value
    ~RegExp(reg_exp, options_exp)
  }

  fn parseDateTime(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseDateTime");
    // Unpack the i32 value
    let value = ~DateTime(data[*index] as u64);
    // Adjust index
    *index += 8;
    // Return the value
    value
  }

  fn parseBoolean(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseBoolean");
    let mut value:bool = true;

    // Check if the value is false
    if data[*index] == 0x00 {
      value = false;
    }

    // Adjust the index
    *index = *index + 1;
    // Return the value
    ~Boolean(value)
  }

  fn parseObjectId(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseObjectId");
    // Allocate a vector
    let mut binary = ~[0, ..12];
    // Copy the data
    copy_memory(binary, slice(data, *index as uint, *index as uint + 12 as uint), 12 as uint);
    // Adjust the index
    *index = *index + 12;
    // Return the value
    ~ObjectId(binary)
  }

  fn parseBinary(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseBinary");
    // unpack the string size
    let size:u32 = data[*index] as u32;
    // Adjust the index
    *index = *index + 4;
    // Get the subtype
    let sub_type:u8 = data[*index] as u8;
    // Adjust the index
    *index = *index + 1;
    // Allocate a vector
    let mut binary = vec::from_elem(size as uint, 0u8);
    // Copy the data
    copy_memory(binary, slice(data, *index as uint, *index as uint + size as uint), size as uint);
    // let mutbinary = slice(data, *index as uint, (*index + (size - 1) as i64) as uint);
    *index = *index + binary.len() as i64;
    // Return the value
    ~Binary(binary, sub_type)
  }

  fn parseDouble(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseDouble");
    // unpack the string size
    let value:f64 = data[*index] as f64;
    // Adjust the index
    *index = *index + 8;
    // Return the value
    ~Double(value)
  }

  fn parseString(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseString");
    // unpack the string size
    let size:u32 = data[*index] as u32;
    // Adjust the index
    *index = *index + 4;
    // unpack the data as a string
    let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
    // Adjust the index
    *index = *index + (size as i64);
    // return string
    ~String(string)
  }

  fn parseInt32(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseInt32");
    // Unpack the i32 value
    let value = ~Int32(data[*index] as i32);
    // Adjust index
    *index += 4;
    // Return the value
    io::println("+++++++++++++++++++++++++++++ parseInt32 1");
    value
  }

  fn parseInt64(index: &mut i64, data: &[u8]) -> ~BsonElement {
    io::println("+++++++++++++++++++++++++++++ parseInt64");
    // Unpack the i32 value
    let value = ~Int64(data[*index] as i64);
    // Adjust index
    *index += 8;
    // Return the value
    value
  }
}

/**
 * Tests
 */
#[test]
fn deserialize_array_test() {
  // {a:[1, {c: mongodb.Long.fromNumber(2)}], d:3}
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x2e, 0x00, 0x00, 0x00, 0x04, 0x61, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x10, 0x30, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x31, 0x00, 0x10, 0x00, 0x00, 0x00, 0x12, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x64, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00]);

  //io::println(fmt!("result = %?", result));

  // fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
  //   match map.find(&~"a") {
  //     Some(&~Int32(number)) => {
  //       assert_eq!(number, 1);
  //     },
  //     _ => fail!()
  //   }
  // }

  // match result {
  //   @Document(~Object(map)) => process_map(map),
  //   @Document(_) => (),
  //   @ParseError(_) => ()
  // }
  //
}

// #[test]
// fn simple_int32_test() {  
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

//   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
//     match map.find(&~"a") {
//       Some(&~Int32(number)) => {
//         assert_eq!(number, 1);
//       },
//       _ => fail!()
//     }
//   }

//   match result {
//     @Document(~Object(map)) => process_map(map),
//     @Document(_) => (),
//     @ParseError(_) => ()
//   }
// }

// #[test]
// fn simple_embedded_doc_serialize_test() {    
//   // {a:{b:1, c: mongodb.Long.fromNumber(2)}, d:3}
//   let parser:BsonParser = BsonParser;

//   // Build a BSON object
//   let map_inner = @mut TreeMap::new::<~str, ~BsonElement>();
//   map_inner.insert(~"c", ~Int64(2));
//   map_inner.insert(~"b", ~Int32(1));

//   let map = @mut TreeMap::new::<~str, ~BsonElement>();
//   map.insert(~"d", ~Int32(3));
//   map.insert(~"a", ~Object(map_inner));
//   let object = ~Object(map);

//   // Serialize the object
//   let data = parser.serialize(object);
//   let expectedData = ~[0x26, 0x00, 0x00, 0x00, 0x03, 0x61, 0x00, 0x17, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x64, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00];

//   // Validate equality
//   assert_eq!(data, expectedData);
// }


// #[test]
// fn simple_int32_serialize_test() {    
//   let parser:BsonParser = BsonParser;
//   // Build a BSON object
//   let map = @mut TreeMap::new::<~str, ~BsonElement>();
//   map.insert(~"a", ~Int32(1));
//   let object:~BsonElement = ~Object(map);
//   // Serialize the object
//   let data = parser.serialize(object);
//   let expectedData = ~[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
//   // Validate equality
//   assert_eq!(data, expectedData);
// }

// #[test]
// fn simple_string_test() {  
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x18, 0x00, 0x00, 0x00, 0x02, 0x61, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00]);

//   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
//     match map.find(&~"a") {
//       Some(&~String(final)) => {
//         assert_eq!(final, @"hello world");
//       },
//       _ => fail!()
//     }
//   }

//   match result {
//     @Document(~Object(map)) => process_map(map),
//     @Document(_) => (),
//     @ParseError(_) => ()
//   }
// }

// #[test]
// fn simple_int64_test() {  
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x10, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

//   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
//     match map.find(&~"a") {
//       Some(&~Int64(number)) => {
//         assert_eq!(number, 2);
//       },
//       _ => fail!()
//     }
//   }

//   match result {
//     @Document(~Object(map)) => process_map(map),
//     @Document(_) => (),
//     @ParseError(_) => ()
//   }
// }

// #[test]
// fn two_value_document_test() {  
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x17, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

//   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
//     match map.find(&~"a") {
//       Some(&~Int64(number)) => assert_eq!(number, 2),
//       _ => ()
//     }

//     match map.find(&~"b") {
//       Some(&~Int32(number)) => assert_eq!(number, 1),
//       _ => ()
//     }
//   }

//   match result {
//     @Document(~Object(map)) => process_map(map),
//     @Document(_) => (),
//     @ParseError(_) => ()
//   }
// }

// #[test]
// fn sub_document_test() {  
//   // {a:{b:1}, c:2}
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x1b, 0x00, 0x00, 0x00, 0x03, 0x61, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00]);

//   // Validate the result
//   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
//     match map.find(&~"a") {
//       Some(&~Object(object_map)) => {

//         // Locate the internal object
//         match object_map.find(&~"b") {
//           Some(&~Int32(number)) => {
//             assert_eq!(number, 1);
//           },
//           _ => fail!()
//         }
//       }
//       _ => fail!()
//     }

//     match map.find(&~"c") {
//       Some(&~Int32(number)) => {
//         assert_eq!(number, 2);
//       },
//       _ => fail!()
//     }
//   }

//   match result {
//     @Document(~Object(map)) => process_map(map),
//     @Document(_) => (),
//     @ParseError(_) => ()
//   }
// }