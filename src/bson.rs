use core::hashmap::linear::LinearMap;
use core::str::raw::from_c_str;

// trait Result {
//   fn is_error(&self) -> bool;
// }

// trait DocumentResult : Result {  
// }

// struct ParseError {
//   error: bool
// }

// impl Result for ParseError {
//   fn is_error(&self) -> bool {
//     true
//   }
// }

// struct Document {
//   map: @mut LinearMap<~str, ~BsonElement>
// }

// impl Document {
//   fn new() -> @Document {
//     @Document {map: @mut LinearMap::new()}
//   }
// }

// impl Result for Document {
//   fn is_error(&self) -> bool {
//     false
//   }
// }

// impl DocumentResult for Document {  
// }

// Available Result values
enum Result {
  Document(@mut LinearMap<~str, ~BsonElement>),
  ParseError(bool)
}

// Available Bson Element types
enum BsonElement {
  Int32(i32),
  Int64(i64)
}

struct BsonParser;
struct ParserState {
  index: u32,
  size: u32,
  document: @Result
}


// //
// // Int 32 element
// struct Int32Element {
//   number: i32
// }

// impl BsonElement for Int32Element {
//   fn typeof() -> BsonType {
//     Int32
//   }
// }

// //
// // Int 64 element
// struct Int64Element {
//   number: i64
// }

// impl BsonElement for Int64Element {
//   fn typeof() -> BsonType {
//     Int64
//   }
// }

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
      return @Document(@mut LinearMap::new());
    }
  
    // Create a document
    let document = @Document(@mut LinearMap::new());
    // Create a parser state
    let state = @mut ParserState {index: 4, size: size, document: document};
  
    while(state.index < state.size - 1) {
      // Let's go, Match on the type
      match data[state.index] as u8 {
        0x02 => io::println("string"),
        0x10 => BsonParser::parseInt32(state, data),
        0x12 => BsonParser::parseInt64(state, data),
        _ => io::println("unknown type")
      }
    }

    // return the document
    document
  }

  fn parseInt32(state:&mut ParserState, data: &[i8]) {
    state.index = state.index + 1;

    // Unpack the cstring
    unsafe {
      // Unpack the name of the field
      let name:~str = from_c_str(&data[state.index]);      
      // Adjust the index
      state.index += name.len() as u32 + 1;
      // Unpack the i32 value
      let value = ~Int32(data[state.index] as i32);
      // Adjust index
      state.index += 4;
      // Add to the map
      match *state.document {
        Document(map) => map.insert(name, value),
        _ => false
      };
    }
  }

  fn parseInt64(state:&mut ParserState, data: &[i8]) {
    state.index = state.index + 1;

    // Unpack the cstring
    unsafe {
      // Unpack the name of the field
      let name:~str = from_c_str(&data[state.index]);      
      // Adjust the index
      state.index += name.len() as u32 + 1;
      // Unpack the i64 value
      let value = ~Int64(data[state.index] as i64);
      // Adjust index
      state.index += 8;
      // Add to the map
      match *state.document {
        Document(map) => map.insert(name, value),
        _ => false
      };
    }
  }
}

#[test]
fn simple_int32_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

  fn process_map(map: @mut LinearMap<~str, ~BsonElement>) {
    match map.find(&~"a") {
      Some(&~Int32(number)) => assert_eq!(number, 1),
      _ => ()
    }
  }

  match result {
    @Document(map) => process_map(map),
    @ParseError(_) => ()
  }
}

#[test]
fn two_value_document_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x17, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x62, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);

  fn process_map(map: @mut LinearMap<~str, ~BsonElement>) {
    match map.find(&~"a") {
      Some(&~Int64(number)) => assert_eq!(number, 2),
      _ => ()
    }

    match map.find(&~"b") {
      Some(&~Int32(number)) => assert_eq!(number, 1),
      _ => ()
    }
  }

  match result {
    @Document(map) => process_map(map),
    @ParseError(_) => ()
  }
}

// fn process_map(map: @mut LinearMap<~str, ~BsonElement>) {        
//   for map.each_key |key| {
//     let value = map.find(key);
//     io::println(fmt!(" %?", value));
    
//     match map.find(key) {
//       Some(&~Int64(number)) => assert_eq!(2, number),
//       _ => io::println("found nothing"),
//     }
//   }
// }
