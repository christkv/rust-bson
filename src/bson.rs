use core::hashmap::linear::LinearMap;
use core::str::raw::from_c_str;

// Available Result values
enum Result {
  Document(@BsonElement),
  ParseError(bool)
}

// Available Bson Element types
enum BsonElement {
  Int32(i32),
  Int64(i64),
  Object(@mut LinearMap<~str, @BsonElement>),
}

struct BsonParser;
struct ParserState {
  index: u32,
  document: @BsonElement
}

impl BsonParser {
  fn deserialize(&self, data: &[i8]) -> @Result {
    // Get the initial state of the parsing
    let size = data[0] as u32;
    // Return an error if we the sizes of the message are not the same
    if size != data.len() as u32 {
      // return @ParseError {error: true} as @Result;
      return @ParseError(true);
    }

    // If we have zero elements (special case)
    if(size == 5) {
      return @Document(@Object(@mut LinearMap::new()));
    }


    // // Parse the document
    // let object = @Object(@mut LinearMap::new());
    // // Create a parser state
    // let state = @mut ParserState {index: 4, document: object};

    // Parse the document
    let object = BsonParser::deserialize_loop(data, @mut 0i64);
    // return the document
    @Document(object)
  }

  fn deserialize_loop(data: &[i8], index: &mut i64) -> @BsonElement {  
    // io::println("======= deserialize_loop");
    // Create an empty object
    let object: @BsonElement = @Object(@mut LinearMap::new());
    // Decode the document size
    let size = data[*index] as u32;
    // Adjust the location of the index
    *index = *index + 4;

    // // Get the top level object
    // let object = @Object(@mut LinearMap::new());
    // Loop until we are done
    loop {
      // Get bson type
      let bson_type = data[*index];
      // Adjust to name of the field
      *index += 1;
      // Decode the name from the cstring
      let name = BsonParser::extract_string(*index, data); 
      // Adjust the index to point to the data
      *index += name.len() as i64 + 1;

      // Access the internal map
      match object {
        @Object(map) => {
          match bson_type as u8 {
            0x02 => io::println("string"),
            0x10 => {
                map.insert(name, BsonParser::parseInt32(index, data));
              },
            0x12 => {
                map.insert(name, BsonParser::parseInt64(index, data));
              },
            0x03 => {
                // Create a new object
                let new_object = BsonParser::deserialize_loop(data, index);
                // Add to the map
                map.insert(name, new_object);
              }
            _ => ()
          }
        }
        _ => ()
      };
      break;      
    }

    object
  }

  fn extract_string(index:i64, data: &[i8]) -> ~str {
    unsafe {
      // Unpack the name of the field
      from_c_str(&data[index])
    }
  }

  fn parseInt32(index: &mut i64, data: &[i8]) -> @BsonElement {
    // Unpack the i32 value
    let value = @Int32(data[*index] as i32);
    // Adjust index
    *index += 4;
    // Return the value
    value
  }

  fn parseInt64(index: &mut i64, data: &[i8]) -> @BsonElement {
    // Unpack the i32 value
    let value = @Int64(data[*index] as i64);
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
fn simple_int32_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

  fn process_map(map: @mut LinearMap<~str, @BsonElement>) {
    match map.find(&~"a") {
      Some(&@Int32(number)) => {
        assert_eq!(number, 1);
      },
      _ => fail!()
    }
  }

  match result {
    @Document(@Object(map)) => process_map(map),
    @Document(_) => (),
    @ParseError(_) => ()
  }
}

#[test]
fn simple_int64_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x10, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

  fn process_map(map: @mut LinearMap<~str, @BsonElement>) {
    match map.find(&~"a") {
      Some(&@Int64(number)) => {
        assert_eq!(number, 2);
      },
      _ => fail!()
    }
  }

  match result {
    @Document(@Object(map)) => process_map(map),
    @Document(_) => (),
    @ParseError(_) => ()
  }
}

#[test]
fn two_value_document_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x17, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

  fn process_map(map: @mut LinearMap<~str, @BsonElement>) {
    match map.find(&~"a") {
      Some(&@Int64(number)) => assert_eq!(number, 2),
      _ => ()
    }

    match map.find(&~"b") {
      Some(&@Int32(number)) => assert_eq!(number, 1),
      _ => ()
    }
  }

  match result {
    @Document(@Object(map)) => process_map(map),
    @Document(_) => (),
    @ParseError(_) => ()
  }
}

#[test]
fn sub_document_test() {  
  // {a:{b:1}}
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x1b, 0x00, 0x00, 0x00, 0x03, 0x61, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x63, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00]);

  // Validate the result
  fn process_map(map: @mut LinearMap<~str, @BsonElement>) {
    match map.find(&~"a") {
      Some(&@Object(object_map)) => {

        // Locate the internal object
        match object_map.find(&~"b") {
          Some(&@Int32(number)) => {
            assert_eq!(number, 1);
          },
          _ => fail!()
        }
      }
      _ => fail!()
    }
  }

  match result {
    @Document(@Object(map)) => process_map(map),
    @Document(_) => (),
    @ParseError(_) => ()
  }
}