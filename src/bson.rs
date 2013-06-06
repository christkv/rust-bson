#[link(name = "bson",
       vers = "0.1",
       uuid = "812ef35d-c3f5-4d94-9199-a2346bfa346e")];
#[crate_type = "lib"];
extern mod extra;
extern mod std;

use std::*;
use std::str::from_bytes;
use extra::treemap::TreeMap;
// use

use std::io::{WriterUtil,ReaderUtil};

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
  reader: @io::Reader
}

pub impl Decoder {
  fn new(reader: @io::Reader) -> Decoder {
    Decoder {
      reader: reader
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
        0x07 => { map.insert(name, @ObjectId(at_vec::to_managed(self.reader.read_bytes(12)))); },
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
    let bytes = at_vec::to_managed(self.reader.read_bytes(binary_size as uint));
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

/*
 * BSON Encoder
 */
pub struct Encoder {
  writer: @io::Writer
}

pub impl Encoder {
  fn new(writer: @io::Writer) -> Encoder {
    Encoder {
      writer: writer
    }
  }

  fn encode(&self, object: &BsonElement) {
    self.encodeObject(object);
  }

  #[inline(always)]
  priv fn encodeObjectValue(&self, k:&str, object:&BsonElement) {
    // Write the type data
    self.writer.write_u8(BsonObject as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Encode the object
    self.encodeObject(object);    
  }

  #[inline(always)]
  priv fn encodeMaxKey(&self, k:&str) {
    // Write the type data
    self.writer.write_u8(BsonMaxKey as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeMinKey(&self, k:&str) {
    // Write the type data
    self.writer.write_u8(BsonMinKey as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeTimestamp(&self, k:&str, timestamp:u64) {
    // Write the type data
    self.writer.write_u8(BsonTimestamp as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the regexp
    self.writer.write_le_u64(timestamp);    
  }

  #[inline(always)]
  priv fn encodeSymbol(&self, k:&str, symbol:&str) {
    // Write the type data
    self.writer.write_u8(BsonSymbol as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the regexp
    self.writer.write_le_u32(symbol.len() as u32 + 1);
    self.writer.write_str(symbol);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeJavascript(&self, k:&str, code:&str) {
    // Write the type data
    self.writer.write_u8(BsonJavascriptCode as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the regexp
    self.writer.write_le_u32(code.len() as u32 + 1);
    self.writer.write_str(code);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeRegExp(&self, k:&str, regexp:&str, options:&str) {
    // Write the type data
    self.writer.write_u8(BsonRegexp as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the regexp
    self.writer.write_str(regexp);
    self.writer.write_u8(0x00);
    self.writer.write_str(options);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeNull(&self, k:&str) {
    // Write the type data
    self.writer.write_u8(BsonNull as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
  }  

  #[inline(always)]
  priv fn encodeDateTime(&self, k:&str, date_time:u64) {
    // Write the type data
    self.writer.write_u8(BsonDate as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the datetime
    self.writer.write_le_u64(date_time);
  }

  #[inline(always)]
  priv fn encodeBoolean(&self, k:&str, boolean:bool) {
    // Write the type data
    self.writer.write_u8(BsonBoolean as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the objectid
    if boolean {
      self.writer.write_u8(0x01);
    } else {
      self.writer.write_u8(0x00);                
    }
  }

  #[inline(always)]
  priv fn encodeObjectId(&self, k:&str, id:&[u8]) {
    // Write the type data
    self.writer.write_u8(BsonObjectId as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the objectid
    self.writer.write(id);
  }

  #[inline(always)]
  priv fn encodeUndefined(&self, k:&str) {
    // Write the type data
    self.writer.write_u8(BsonUndefined as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeBinary(&self, k:&str, data:&[u8], sub_type:u8) {
    // Write the type data
    self.writer.write_u8(BsonBinary as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the field name
    self.writer.write_le_u32(data.len() as u32);
    self.writer.write_u8(sub_type);              
    self.writer.write(data);
  }

  #[inline(always)]
  priv fn encodeString(&self, k:&str, string:&str) {
    // Write the type data
    self.writer.write_u8(BsonString as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the field name
    self.writer.write_le_u32(string.len() as u32 + 1);
    self.writer.write_str(string);
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeDouble(&self, k:&str, number:f64) {
    // Write the type data
    self.writer.write_u8(BsonDouble as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the value out
    self.writer.write_le_f64(number);
  } 

  #[inline(always)]
  priv fn encodeInt32(&self, k:&str, number:i32) {
    // Write the type data
    self.writer.write_u8(BsonInt32 as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the value out
    self.writer.write_le_i32(number);
  }

  #[inline(always)]
  priv fn encodeInt64(&self, k:&str, number:i64) {
    // Write the type data
    self.writer.write_u8(BsonInt64 as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Write the value out
    self.writer.write_le_i64(number);
  }

  #[inline(always)]
  priv fn encodeJavascriptWScope(&self, k:&str, code:&str, object:&BsonElement) {
    // Write the type data
    self.writer.write_u8(BsonJavascriptCodeWScope as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Save start position
    let startPosition = self.writer.tell();
    // Write place holder for the size
    self.writer.write_le_u32(0u32);
    // Let's write the code out
    self.writer.write_le_u32(code.len() as u32 + 1);
    self.writer.write_str(code);
    self.writer.write_u8(0x00);
    // Write the document
    self.encodeObject(object);
    // Save current position
    let endPosition = self.writer.tell();
    // Seek an write the data
    self.writer.seek(startPosition as int, io::SeekSet);
    // Write the size
    self.writer.write_le_u32((endPosition - startPosition + 1) as u32);
    // Return to write position
    self.writer.seek(endPosition as int, io::SeekSet);
  }

  #[inline(always)]
  priv fn encodeArray(&self, k:&str, array:&[@BsonElement]) {
    // Write the type data
    self.writer.write_u8(BsonArray as u8);
    // Write the field name
    self.writer.write_str(k);
    self.writer.write_u8(0x00);
    // Save start position
    let startPosition = self.writer.tell();
    // Write place holder for the size
    self.writer.write_le_u32(0u32);
    // The mutable
    let mut index = 0;
    // Let's iterate over all the array values
    array.each(|&value| {
      let k = int::to_str(index);
      index = index + 1;
      // Match on the value to serialize
      match value {
        @Array(array) => self.encodeArray(k, array),
        @Object(_) => self.encodeObjectValue(k, value),
        @MaxKey => self.encodeMaxKey(k),
        @MinKey => self.encodeMinKey(k),
        @Timestamp(timestamp) => self.encodeTimestamp(k, timestamp),
        @Symbol(symbol) =>  self.encodeSymbol(k, symbol),
        @JavascriptCode(code) => self.encodeJavascript(k, code),
        @RegExp(regexp, options) => self.encodeRegExp(k, regexp, options),
        @Null => self.encodeNull(k),
        @DateTime(date_time) => self.encodeDateTime(k, date_time),
        @Boolean(boolean) => self.encodeBoolean(k, boolean),
        @ObjectId(id) => self.encodeObjectId(k, id),
        @Undefined => self.encodeUndefined(k),
        @Binary(data, sub_type) => self.encodeBinary(k, data, sub_type),
        @String(string) => self.encodeString(k, string),
        @Double(number) => self.encodeDouble(k, number), 
        @Int32(number) => self.encodeInt32(k, number), 
        @Int64(number) => self.encodeInt64(k, number),
        _ => ()
      }

      true
    });

    // Save current position
    let endPosition = self.writer.tell();
    // Seek an write the data
    self.writer.seek(startPosition as int, io::SeekSet);
    // Write the size
    self.writer.write_le_u32((endPosition - startPosition + 1) as u32);
    // Return to write position
    self.writer.seek(endPosition as int, io::SeekSet);
    // Write terminating null for the object
    self.writer.write_u8(0x00);
  }

  #[inline(always)]
  priv fn encodeObject(&self, object:&BsonElement) {
    // Unpack the object
    match object {
      &Object(map) => {
        let startPosition = self.writer.tell();
        // Write place holder for the size
        self.writer.write_le_u32(0u32);
        // Iterate over all the fields
        for map.each_key |k| {
          // Get the value
          let value = map.find(k);
          // Match on the value
          match value {
            Some(&@Array(array)) => self.encodeArray(*k, array),
            Some(&@Object(_)) => {
              match value {
                Some(&object) => self.encodeObjectValue(*k, object),
                _ => ()
              }
            },
            Some(&@MaxKey) => self.encodeMaxKey(*k),
            Some(&@MinKey) => self.encodeMinKey(*k),
            Some(&@Timestamp(timestamp)) => self.encodeTimestamp(*k, timestamp),
            Some(&@Symbol(symbol)) =>  self.encodeSymbol(*k, symbol),
            Some(&@JavascriptCode(code)) => self.encodeJavascript(*k, code),
            Some(&@RegExp(regexp, options)) => self.encodeRegExp(*k, regexp, options),
            Some(&@Null) => self.encodeNull(*k),
            Some(&@DateTime(date_time)) => self.encodeDateTime(*k, date_time),
            Some(&@Boolean(boolean)) => self.encodeBoolean(*k, boolean),
            Some(&@ObjectId(id)) => self.encodeObjectId(*k, id),
            Some(&@Undefined) => self.encodeUndefined(*k),
            Some(&@Binary(data, sub_type)) => self.encodeBinary(*k, data, sub_type),
            Some(&@String(string)) => self.encodeString(*k, string),
            Some(&@Double(number)) => self.encodeDouble(*k, number), 
            Some(&@Int32(number)) => self.encodeInt32(*k, number), 
            Some(&@Int64(number)) => self.encodeInt64(*k, number),
            Some(&@JavascriptCodeWScope(code, doc)) => self.encodeJavascriptWScope(*k, code, doc),
            _ => ()
          }
        };

        // Save current position
        let endPosition = self.writer.tell();
        // Seek an write the data
        self.writer.seek(startPosition as int, io::SeekSet);
        // Write the size
        self.writer.write_le_u32((endPosition - startPosition + 1) as u32);
        // Return to write position
        self.writer.seek(endPosition as int, io::SeekSet);
        // Write terminating null for the object
        self.writer.write_u8(0x00);
      },
      _ => ()
    }
  }
}

#[test]
fn serialize_full_document() {
  let data = io::with_bytes_writer(|wd| {
    // Create a simple object to encode
    let map = @mut TreeMap::new::<~str, @BsonElement>();
    map.insert(~"1", @Double(33.3333));
    map.insert(~"2", @String(@"Hello world"));

    // Insert additional object
    let map2 = @mut TreeMap::new::<~str, @BsonElement>();
    map2.insert(~"a", @String(@"Embedded string"));
    map.insert(~"3", @Object(map2));

    // Insert array
    map.insert(~"4", @Array(@[@String(@"Hello world"), @Int32(200)]));

    // Add basic types    
    map.insert(~"5", @Binary(@[1, 1, 1, 1], 0));
    // map.insert(~"6", @Undefined);
    map.insert(~"7", @ObjectId(@[2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]));
    map.insert(~"8", @Boolean(true));
    map.insert(~"9", @DateTime(32233u64));
    map.insert(~"10", @Null);
    map.insert(~"11", @RegExp(@"regexp", @""));
    map.insert(~"12", @JavascriptCode(@"function() {}"));
    map.insert(~"13", @Symbol(@"symbol"));

    // Create Javascript with scope
    let map3 = @mut TreeMap::new::<~str, @BsonElement>();
    map3.insert(~"a", @Int32(1));
    map.insert(~"14", @JavascriptCodeWScope(@"function() {}", @Object(map3)));

    // // map.insert(~"14", @Symbol(@"symbol"));
    map.insert(~"15", @Int32(100));
    map.insert(~"16", @Timestamp(100000u64));
    map.insert(~"17", @Int64(22222i64));
    map.insert(~"18", @MinKey);
    map.insert(~"19", @MaxKey);

    // Create encoder instance
    let encoder = Encoder::new(wd);
    // Encode the data
    encoder.encode(@Object(map));
  });

  // Expected serialization bytes
  let expected_data = ~[17, 1, 0, 0, 1, 49, 0, 223, 224, 11, 147, 169, 170, 64, 64, 10, 49, 48, 0, 11, 49, 49, 0, 114, 101, 103, 101, 120, 112, 0, 0, 13, 49, 50, 0, 14, 0, 0, 0, 102, 117, 110, 99, 116, 105, 111, 110, 40, 41, 32, 123, 125, 0, 14, 49, 51, 0, 7, 0, 0, 0, 115, 121, 109, 98, 111, 108, 0, 15, 49, 52, 0, 35, 0, 0, 0, 14, 0, 0, 0, 102, 117, 110, 99, 116, 105, 111, 110, 40, 41, 32, 123, 125, 0, 12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0, 16, 49, 53, 0, 100, 0, 0, 0, 17, 49, 54, 0, 160, 134, 1, 0, 0, 0, 0, 0, 18, 49, 55, 0, 206, 86, 0, 0, 0, 0, 0, 0, 255, 49, 56, 0, 127, 49, 57, 0, 2, 50, 0, 12, 0, 0, 0, 72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 3, 51, 0, 28, 0, 0, 0, 2, 97, 0, 16, 0, 0, 0, 69, 109, 98, 101, 100, 100, 101, 100, 32, 115, 116, 114, 105, 110, 103, 0, 0, 4, 52, 0, 31, 0, 0, 0, 2, 48, 0, 12, 0, 0, 0, 72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 16, 49, 0, 200, 0, 0, 0, 0, 5, 53, 0, 4, 0, 0, 0, 0, 1, 1, 1, 1, 7, 55, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 8, 56, 0, 1, 9, 57, 0, 233, 125, 0, 0, 0, 0, 0, 0, 0];
  // Assert correctness
  assert_eq!(data, expected_data);
}

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