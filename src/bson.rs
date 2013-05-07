#[link(name = "bson",
       vers = "0.1",
       uuid = "812ef35d-c3f5-4d94-9199-a2346bfa346e")];
#[crate_type = "lib"];
extern mod std;

// use core::str::raw::from_c_str;
use core::str::from_bytes;
// use core::vec::const_slice;
// use core::vec::slice;
// use core::cast::transmute;
// use core::vec::bytes::copy_memory;
use std::treemap::TreeMap;

use core::io::{WriterUtil,ReaderUtil};
use std::serialize;

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

// // Type used to wrap array item
// struct ArrayItem {
//   item: ~BsonElement
// }

// Available Bson Element types
enum BsonElement {
  Double(f64),
  String(~str),
  Object(@mut TreeMap<~str, ~BsonElement>),
  Array(@mut TreeMap<~str, ~BsonElement>),  
  Binary(~[u8], u8),
  Undefined,
  ObjectId(~[u8]),
  Boolean(bool),
  DateTime(u64),
  Null,
  RegExp(~str, ~str),
  JavascriptCode(~str),
  Symbol(~str),
  JavascriptCodeWScope(~str, ~BsonElement),  
  Int32(i32),
  Timestamp(u64),
  Int64(i64),  
  MinKey,
  MaxKey
}

// pub struct Encoder {
//   writer: @io::Writer
// }

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

  // fn extract_string(index:i64, data: &[u8]) -> ~str {
  //   unsafe {
  //     let data2: &[i8] = transmute(data);
  //     // Unpack the name of the field
  //     from_c_str(&data2[index])
  //   }
  // }

  // BsonObject = 0x03,
  // BsonArray = 0x04,

  priv fn parse_object(&self) -> ~BsonElement {
    // Read object size
    self.reader.read_le_u32();
    // Store all the values of the object in order
    let map = @mut TreeMap::new::<~str, ~BsonElement>();

    // Loop over all items in the object
    loop {
      // Read the object type
      let bson_type:u8 = self.reader.read_u8();
      // If bson_type == 0 we are done
      if bson_type == 0 || bson_type == 255 {
        break;
      }
      
      // Decode the name from the cstring
      let name = self.reader.read_c_str();
      // io::println(fmt!(" + deserialize %? :: of type %?", name, bson_type));
      
      // Match on the type
      match bson_type {
        0x01 => { map.insert(name, self.parse_double()); },
        0x02 => { map.insert(name, self.parse_string()); },
        0x03 => { map.insert(name, self.parse_object()); },
        0x04 => { 
          match self.parse_object() {
            ~Object(map) => { map.insert(name, ~Array(map)); },
            _ => ()
          }
        },
        0x05 => {  map.insert(name, self.parse_binary()); },
        0x06 => { map.insert(name, ~Undefined); },
        0x07 => { map.insert(name, ~ObjectId(self.reader.read_bytes(12))); },
        0x08 => { map.insert(name, self.parse_bool()); },
        0x09 => { map.insert(name, ~DateTime(self.reader.read_le_u64())); },
        0x0a => { map.insert(name, ~Null); },
        0x0b => { map.insert(name, self.parse_regexp()); },
        0x0d => { map.insert(name, self.parse_javascript()); },
        0x0e => { map.insert(name, self.parse_symbol()); },
        0x0f => { map.insert(name, self.parse_javascript_w_scope()); },
        0x10 => { map.insert(name, ~Int32(self.reader.read_le_i32())); },
        0x11 => { map.insert(name, ~Timestamp(self.reader.read_le_u64())); },
        0x12 => { map.insert(name, ~Int64(self.reader.read_le_i64())); },
        0xff => { map.insert(name, ~MinKey); },
        0x7f => { map.insert(name, ~MaxKey); },
        _ => fail!(~"Invalid bson type")
      }      
    }

    ~Object(map)
  }

  #[inline(always)]
  priv fn parse_double(&self) -> ~BsonElement {
    // Read u64 value
    let u64_value = self.reader.read_le_u64();
    // Cast to f64
    let value = to_double(u64_value);
    // Insert value
    ~Double(value)
  }

  #[inline(always)]
  priv fn parse_string(&self) -> ~BsonElement {
    let string_size = self.reader.read_le_u32();
    let bytes = self.reader.read_bytes(string_size as uint);
    ~String(from_bytes(bytes))
  }

  #[inline(always)]
  priv fn parse_binary(&self) -> ~BsonElement {
    let binary_size = self.reader.read_le_u32();
    let sub_type = self.reader.read_u8();
    let bytes = self.reader.read_bytes(binary_size as uint);
    ~Binary(bytes, sub_type)
  }

  #[inline(always)]
  priv fn parse_bool(&self) -> ~BsonElement {
    let boolValue = self.reader.read_u8();
    if boolValue == 0 {
      ~Boolean(false)
    } else {
      ~Boolean(true)
    }
  }

  #[inline(always)]
  priv fn parse_regexp(&self) -> ~BsonElement {
    let reg_exp = self.reader.read_c_str();
    let options = self.reader.read_c_str();
    ~RegExp(reg_exp, options) 
  }

  #[inline(always)]
  priv fn parse_symbol(&self) -> ~BsonElement {
    let string_size = self.reader.read_le_u32();
    let bytes = self.reader.read_bytes(string_size as uint);
    ~Symbol(from_bytes(bytes))
  }

  #[inline(always)]
  priv fn parse_javascript_w_scope(&self) -> ~BsonElement {
    let string_size = self.reader.read_le_u32();
    let bytes = self.reader.read_bytes(string_size as uint);
    let document = self.parse_object();
    ~JavascriptCodeWScope(from_bytes(bytes), document) 
  }

  #[inline(always)]
  priv fn parse_javascript(&self) -> ~BsonElement {
    let string_size = self.reader.read_le_u32();
    let bytes = self.reader.read_bytes(string_size as uint);
    ~JavascriptCode(from_bytes(bytes))
  }

  fn parse(&self) -> ~BsonElement {
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
  // var doc2 = {
  //   'string': 'hello',
  //   'array': [1,2,3],
  //   'hash': {'a':1, 'b':2},
  //   'date': date,
  //   'oid': oid,
  //   'binary': bin,
  //   'int': 42,
  //   'float': 33.3333,
  //   'regexp': /regexp/,
  //   'boolean': true,
  //   'long': date.getTime(),
  //   'where': new Code('this.a > i', {i:1}),
  // }

  let data = @[236,0,0,0,2,115,116,114,105,110,103,0,6,0,0,0,104,101,108,108,111,0,4,97,114,114,97,121,0,26,0,0,0,16,48,0,1,0,0,0,16,49,0,2,0,0,0,16,50,0,3,0,0,0,0,3,104,97,115,104,0,19,0,0,0,16,97,0,1,0,0,0,16,98,0,2,0,0,0,0,9,100,97,116,101,0,187,62,201,126,62,1,0,0,7,111,105,100,0,81,136,231,190,131,156,66,3,106,0,0,17,5,98,105,110,97,114,121,0,9,0,0,0,0,98,105,110,115,116,114,105,110,103,16,105,110,116,0,42,0,0,0,1,102,108,111,97,116,0,223,224,11,147,169,170,64,64,11,114,101,103,101,120,112,0,114,101,103,101,120,112,0,0,8,98,111,111,108,101,97,110,0,1,1,108,111,110,103,0,0,176,235,147,236,231,115,66,15,119,104,101,114,101,0,31,0,0,0,11,0,0,0,116,104,105,115,46,97,32,62,32,105,0,12,0,0,0,16,105,0,1,0,0,0,0,0];
  io::with_bytes_reader(data, |rd| {
    let decoder = Decoder::new(rd);  
    let obj = decoder.parse();
    io::println(fmt!("%?", obj));
  });
  // let parser:BsonParser = BsonParser;
  // let result = parser.deserialize();
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

//   fn deserialize(&self, data: &[u8]) -> @Result {
//     // Get the initial state of the parsing
//     let size = data[0] as u32;
//     // Return an error if we the sizes of the message are not the same
//     if size != data.len() as u32 {
//       // return @ParseError {error: true} as @Result;
//       return @ParseError(true);
//     }

//     // If we have zero elements (special case)
//     if(size == 5) {
//       return @Document(~Object(@mut TreeMap::new::<~str, ~BsonElement>()));
//     }

//     // Parse the document
//     let object = BsonParser::deserialize_loop(data, @mut 0i64, false);
//     io::println("+++++++++++++++++++++++++++++ deserialize");
//     // return the document
//     @Document(object)
//   }

//   fn deserialize_loop(data: &[u8], index: &mut i64, is_array:bool) -> ~BsonElement {  
//     io::println("+++++++++++++++++++++++++++++ deserialize_object");
//     // Create an empty object
//     let map = @mut TreeMap::new::<~str, ~BsonElement>();
//     // Adjust the location of the index
//     *index = *index + 4;

//     // Loop until we are done
//     loop {
//       // Get bson type
//       let bson_type = data[*index];
//       // Adjust to name of the field
//       *index += 1;
//       // If type is 0x00 we are done
//       if bson_type == 0x00 {
//         break;        
//       }

//       // io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? :: %?", bson_type, *index));

//       // Decode the name from the cstring
//       let name = BsonParser::extract_string(*index, data); 
//       // Adjust the index to point to the data
//       *index += name.len() as i64 + 1;
//       io::println(fmt!(" name = %?", name));

//       // Match bson type
//       match bson_type as u8 {
//         0x01 => { map.insert(name, BsonParser::parseDouble(index, data)); },
//         0x02 => { map.insert(name, BsonParser::parseString(index, data)); },
//         0x03 => { map.insert(name, BsonParser::deserialize_loop(data, index, false)); },
//         0x04 => { map.insert(name, BsonParser::deserialize_loop(data, index, true)); },
//         0x05 => { map.insert(name, BsonParser::parseBinary(index, data)); },
//         0x06 => { map.insert(name, ~Undefined); },
//         0x07 => { map.insert(name, BsonParser::parseObjectId(index, data)); },
//         0x08 => { map.insert(name, BsonParser::parseBoolean(index, data)); },
//         0x09 => { map.insert(name, BsonParser::parseDateTime(index, data)); },
//         0x0a => { map.insert(name, ~Null); },
//         0x0b => { map.insert(name, BsonParser::parseRegExp(index, data)); },
//         0x0d => { map.insert(name, BsonParser::parseJavaScriptCode(index, data)); },
//         0x0e => { map.insert(name, BsonParser::parseSymbol(index, data)); },
//         0x0f => { map.insert(name, BsonParser::parseJavaScriptCodeWScope(index, data)); },
//         0x10 => { map.insert(name, BsonParser::parseInt32(index, data)); },
//         0x11 => { map.insert(name, BsonParser::parseTimestamp(index, data)); },
//         0x12 => { map.insert(name, BsonParser::parseInt64(index, data)); },
//         0xff => { map.insert(name, ~MinKey); },
//         0x7f => { map.insert(name, ~MaxKey); },
//         _ => ()
//       }

//       // io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? :: %? ---", bson_type, *index));

//     }

//       // io::println(fmt!("+++++++++++++++++++++++++++++ deserialize_object :: %? --- end", *index));

//     match is_array {
//       true => ~Array(map),
//       _ => ~Object(map)
//     }
//   }
// // 
//   fn extract_string(index:i64, data: &[u8]) -> ~str {
//     unsafe {
//       let data2: &[i8] = transmute(data);
//       // Unpack the name of the field
//       from_c_str(&data2[index])
//     }
//   }

//   fn parseTimestamp(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseTimestamp");
//     // Unpack the i32 value
//     let value = ~Timestamp(data[*index] as u64);
//     // Adjust index
//     *index += 8;
//     // Return the value
//     value
//   }

//   fn parseSymbol(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseSymbol");
//     // unpack the string size
//     let size:u32 = data[*index] as u32;
//     // Adjust the index
//     *index = *index + 4;
//     // unpack the data as a string
//     let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
//     io::println(fmt!("%?", string));
//     // Adjust the index
//     *index = *index + (size as i64);
//     // return string
//     ~Symbol(string)
//   }

//   fn parseJavaScriptCodeWScope(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseJavaScriptCodeWScope");
//     // unpack the string size
//     let size:u32 = data[*index] as u32;
//     io::println(fmt!("%? :: %? :: %?", size, *index, data.len()));
//     // Adjust the index
//     *index = *index + 4;
//     // unpack the data as a string
//     let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
//     io::println(fmt!("%?", string));
//     // Adjust the index
//     *index = *index + (size as i64);
//     // Parse the document
//     let document = BsonParser::deserialize_loop(data, index, false);
//     // Return the value
//     ~JavascriptCodeWScope(string, document)
//   }

//   fn parseJavaScriptCode(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseJavaScriptCode");
//     // unpack the string size
//     let size:u32 = data[*index] as u32;
//     // Adjust the index
//     *index = *index + 4;
//     // unpack the data as a string
//     let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
//     // Adjust the index
//     *index = *index + (size as i64);
//     // return string
//     ~JavascriptCode(string)
//   }

//   fn parseRegExp(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseRegExp");
//     // Decode the regexp from the cstring
//     let reg_exp = BsonParser::extract_string(*index, data); 
//     io::println(fmt!("%?", reg_exp));
//     // Adjust the index to point to the data
//     *index += reg_exp.len() as i64 + 1;
//     // Decode the options
//     let options_exp = BsonParser::extract_string(*index, data);
//     io::println(fmt!("%?", options_exp));
//     // Adjust the index to point to the data
//     *index += options_exp.len() as i64 + 1;
//     // Return the value
//     ~RegExp(reg_exp, options_exp)
//   }

//   fn parseDateTime(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseDateTime");
//     // Unpack the i32 value
//     let value = ~DateTime(data[*index] as u64);
//     io::println(fmt!("%?", value));
//     // Adjust index
//     *index += 8;
//     // Return the value
//     value
//   }

//   fn parseBoolean(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseBoolean");
//     let mut value:bool = true;

//     // Check if the value is false
//     if data[*index] == 0x00 {
//       value = false;
//     }

//     io::println(fmt!("%?", value));

//     // Adjust the index
//     *index = *index + 1;
//     // Return the value
//     ~Boolean(value)
//   }

//   fn parseObjectId(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseObjectId");
//     // Allocate a vector
//     let mut binary = ~[0, ..12];
//     // Copy the data
//     copy_memory(binary, slice(data, *index as uint, *index as uint + 12 as uint), 12 as uint);
//     io::println(fmt!("%?", binary));
//     // Adjust the index
//     *index = *index + 12;
//     // Return the value
//     ~ObjectId(binary)
//   }

//   fn parseBinary(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseBinary");
//     // unpack the string size
//     let size:u32 = data[*index] as u32;
//     // Adjust the index
//     *index = *index + 4;
//     // Get the subtype
//     let sub_type:u8 = data[*index] as u8;
//     // Adjust the index
//     *index = *index + 1;
//     // Allocate a vector
//     let mut binary = vec::from_elem(size as uint, 0u8);
//     // Copy the data
//     copy_memory(binary, slice(data, *index as uint, *index as uint + size as uint), size as uint);
//     io::println(fmt!("%?", sub_type));
//     io::println(fmt!("%?", binary));    
//     // let mutbinary = slice(data, *index as uint, (*index + (size - 1) as i64) as uint);
//     *index = *index + binary.len() as i64;
//     // Return the value
//     ~Binary(binary, sub_type)
//   }

//   priv fn conv_dobule(v:u64) -> f64 {unsafe { cast::transmute(v) }}

//   fn parseDouble(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseDouble");

//         // object[name] = readIEEE754(buffer, index, 'little', 52, 8);

//     // let value:f64 = parseIEEE754(index, data);

//     // unpack the string size
//     let valueU:u64 = data[*index] as u64;
//     // let v = vec::slice(data, *index as uint, *index as uint + 8) as ;
//     let value = conv_dobule(valueU);

//     // unsafe {
//     //   io::println(fmt!("--- %?", v));
//     //   let v2:*float = cast::transmute(vec::raw::to_ptr(v));
//     //   io::println(fmt!("--- %?", v2));
//     // }

//     // let value:f64 = &v as f64;
//     // let value:f64 = num::strconv::from_str_bytes_common(data[*index]);
//     io::println(fmt!("%?", value));
//     // Adjust the index
//     *index = *index + 8;
//     // Return the value
//     ~Double(value)
//   }

//   // fn parseIEEE754(index: &mut i64, data: &[u8]) -> f64 {

//   //   0 as f64
//   // }

//   fn parseString(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseString");
//     // unpack the string size
//     let size:u32 = data[*index] as u32;
//     // Adjust the index
//     *index = *index + 4;
//     // unpack the data as a string
//     let string = from_bytes(const_slice(data, *index as uint, (*index + (size - 1) as i64) as uint)).to_managed();
//     io::println(fmt!("%?", string));
//     // Adjust the index
//     *index = *index + (size as i64);
//     // return string
//     ~String(string)
//   }

//   fn parseInt32(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseInt32");
//     // Unpack the i32 value
//     let value = ~Int32(data[*index] as i32);
//     io::println(fmt!("%?", value));
//     // Adjust index
//     *index += 4;
//     // Return the value
//     value
//   }

//   fn parseInt64(index: &mut i64, data: &[u8]) -> ~BsonElement {
//     io::println("+++++++++++++++++++++++++++++ parseInt64");
//     // Unpack the i32 value
//     let value = ~Int64(data[*index] as i64);
//     io::println(fmt!("%?", value));
//     // Adjust index
//     *index += 8;
//     // Return the value
//     value
//   }
// }



// /**
//  * Tests
//  */
// #[test]
// fn deserialize_full_document() {
//   // var doc2 = {
//   //   'string': 'hello',
//   //   'array': [1,2,3],
//   //   'hash': {'a':1, 'b':2},
//   //   'date': date,
//   //   'oid': oid,
//   //   'binary': bin,
//   //   'int': 42,
//   //   'float': 33.3333,
//   //   'regexp': /regexp/,
//   //   'boolean': true,
//   //   'long': date.getTime(),
//   //   'where': new Code('this.a > i', {i:1}),
//   // }

//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[236,0,0,0,2,115,116,114,105,110,103,0,6,0,0,0,104,101,108,108,111,0,4,97,114,114,97,121,0,26,0,0,0,16,48,0,1,0,0,0,16,49,0,2,0,0,0,16,50,0,3,0,0,0,0,3,104,97,115,104,0,19,0,0,0,16,97,0,1,0,0,0,16,98,0,2,0,0,0,0,9,100,97,116,101,0,187,62,201,126,62,1,0,0,7,111,105,100,0,81,136,231,190,131,156,66,3,106,0,0,17,5,98,105,110,97,114,121,0,9,0,0,0,0,98,105,110,115,116,114,105,110,103,16,105,110,116,0,42,0,0,0,1,102,108,111,97,116,0,223,224,11,147,169,170,64,64,11,114,101,103,101,120,112,0,114,101,103,101,120,112,0,0,8,98,111,111,108,101,97,110,0,1,1,108,111,110,103,0,0,176,235,147,236,231,115,66,15,119,104,101,114,101,0,31,0,0,0,11,0,0,0,116,104,105,115,46,97,32,62,32,105,0,12,0,0,0,16,105,0,1,0,0,0,0,0]);
// }

// // #[test]
// // fn deserialize_array_test() {
// //   let parser:BsonParser = BsonParser;
// //   // {a:[1, {c: mongodb.Long.fromNumber(2)}], d:3}
// //   // let result = parser.deserialize(@[0x2e, 0x00, 0x00, 0x00, 0x04, 0x61, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x10, 0x30, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x31, 0x00, 0x10, 0x00, 0x00, 0x00, 0x12, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x64, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00]);
// //   // {a:[1, 2, 3]}
// //   let result = parser.deserialize(@[0x22, 0x00, 0x00, 0x00, 0x04, 0x61, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x10, 0x30, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x31, 0x00, 0x02, 0x00, 0x00, 0x00, 0x10, 0x32, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00]);

// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~Array(items)) => {
// //         match items.find(&~"0") {
// //           Some(&~Int32(number)) => assert_eq!(number, 1),
// //           _ => fail!()
// //         }

// //         match items.find(&~"1") {
// //           Some(&~Int32(number)) => assert_eq!(number, 2),
// //           _ => fail!()
// //         }

// //         match items.find(&~"2") {
// //           Some(&~Int32(number)) => assert_eq!(number, 3),
// //           _ => fail!()
// //         }
// //       },
// //       _ => fail!()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }  
// // }

// // #[test]
// // fn simple_int32_test() {  
// //   let parser:BsonParser = BsonParser;
// //   let result = parser.deserialize(@[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~Int32(number)) => {
// //         assert_eq!(number, 1);
// //       },
// //       _ => fail!()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }
// // }

// // #[test]
// // fn simple_embedded_doc_serialize_test() {    
// //   // {a:{b:1, c: mongodb.Long.fromNumber(2)}, d:3}
// //   let parser:BsonParser = BsonParser;

// //   // Build a BSON object
// //   let map_inner = @mut TreeMap::new::<~str, ~BsonElement>();
// //   map_inner.insert(~"c", ~Int64(2));
// //   map_inner.insert(~"b", ~Int32(1));

// //   let map = @mut TreeMap::new::<~str, ~BsonElement>();
// //   map.insert(~"d", ~Int32(3));
// //   map.insert(~"a", ~Object(map_inner));
// //   let object = ~Object(map);

// //   // Serialize the object
// //   let data = parser.serialize(object);
// //   let expectedData = ~[0x26, 0x00, 0x00, 0x00, 0x03, 0x61, 0x00, 0x17, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x64, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00];

// //   // Validate equality
// //   assert_eq!(data, expectedData);
// // }


// // #[test]
// // fn simple_int32_serialize_test() {    
// //   let parser:BsonParser = BsonParser;
// //   // Build a BSON object
// //   let map = @mut TreeMap::new::<~str, ~BsonElement>();
// //   map.insert(~"a", ~Int32(1));
// //   let object:~BsonElement = ~Object(map);
// //   // Serialize the object
// //   let data = parser.serialize(object);
// //   let expectedData = ~[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
// //   // Validate equality
// //   assert_eq!(data, expectedData);
// // }

// // #[test]
// // fn simple_string_test() {  
// //   let parser:BsonParser = BsonParser;
// //   let result = parser.deserialize(@[0x18, 0x00, 0x00, 0x00, 0x02, 0x61, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00]);

// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~String(final)) => {
// //         assert_eq!(final, @"hello world");
// //       },
// //       _ => fail!()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }
// // }

// // #[test]
// // fn simple_int64_test() {  
// //   let parser:BsonParser = BsonParser;
// //   let result = parser.deserialize(@[0x10, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~Int64(number)) => {
// //         assert_eq!(number, 2);
// //       },
// //       _ => fail!()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }
// // }

// // #[test]
// // fn two_value_document_test() {  
// //   let parser:BsonParser = BsonParser;
// //   let result = parser.deserialize(@[0x17, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~Int64(number)) => assert_eq!(number, 2),
// //       _ => ()
// //     }

// //     match map.find(&~"b") {
// //       Some(&~Int32(number)) => assert_eq!(number, 1),
// //       _ => ()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }
// // }

// // #[test]
// // fn sub_document_test() {  
// //   // {a:{b:1}, c:2}
// //   let parser:BsonParser = BsonParser;
// //   let result = parser.deserialize(@[0x1b, 0x00, 0x00, 0x00, 0x03, 0x61, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00]);

// //   // Validate the result
// //   fn process_map(map: @mut TreeMap<~str, ~BsonElement>) {
// //     match map.find(&~"a") {
// //       Some(&~Object(object_map)) => {

// //         // Locate the internal object
// //         match object_map.find(&~"b") {
// //           Some(&~Int32(number)) => {
// //             assert_eq!(number, 1);
// //           },
// //           _ => fail!()
// //         }
// //       }
// //       _ => fail!()
// //     }

// //     match map.find(&~"c") {
// //       Some(&~Int32(number)) => {
// //         assert_eq!(number, 2);
// //       },
// //       _ => fail!()
// //     }
// //   }

// //   match result {
// //     @Document(~Object(map)) => process_map(map),
// //     @Document(_) => (),
// //     @ParseError(_) => ()
// //   }
// // }