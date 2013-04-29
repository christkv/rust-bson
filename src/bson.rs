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
      // return Document::new() as @Result;
      return @Document(@mut LinearMap::new());
    }
  
    let document = @Document(@mut LinearMap::new());
    let state = @mut ParserState {index: 4, size: size, document: document};
  
    // Let's go, Match on the type
    match data[state.index] as u8 {
      0x02 => io::println("string"),
      0x10 => BsonParser::parseInt32(state, data),
      0x12 => BsonParser::parseInt64(state, data),
      _ => io::println("unknown type")
    }

// io::println("==================================");

    // while state.index < 

    // Decode the size of the total message
    // let size : Option<i32> = i32::parse_bytes(vec::slice(data, 0, 4), 10u);

    // let number_data = to_bytes(~"123");
    // let number_data = vec::slice(data, 0, 4) as &i32;
    // let number_data = ~[0x0c, 0, 0];
    // let size = match i32::parse_bytes(number_data, 10u) {
    //   Some(cn) => cn as int,
    //   None => fail!(fmt!("internal error: parse_def_id: crate number \
    //                            expected, but found %?", number_data))
    // };

    // let number_data = vec::slice(data, 0, 4);
    // let size = number_data[0] as i32;

    // io::println(fmt!("data :: %?", number_data));
    // io::println(fmt!("size :: %?", size));
    // io::println(fmt!("state :: %?", state));
    // io::println(fmt!("length :: %?", data.len()));

    // let error = @ParseError {error: false};
    // let mut map = ~LinearMap::new();
    // let mut document = ~Document {map: map};
    // let element = ~Int32Element {number: 1};
    // document.insert(~"hello", element as ~BsonElement);

    // document.map.insert(~"hello", element as ~BsonElement);
    // map.insert(~"hello", ~"world");
    
    // document as @Result
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

      // // Save field in the document
      // state.document.map.insert(name, value);
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

// #[test]
// fn simple_int32_test() {  
//   let parser:BsonParser = BsonParser;
//   let result = parser.deserialize(@[0x0c, 0x00, 0x00, 0x00, 0x10, 0x61, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
//   io::println(fmt!("result :: %?", result.is_error()));
// }

#[test]
fn simple_int64_test() {  
  let parser:BsonParser = BsonParser;
  let result = parser.deserialize(@[0x10, 0x00, 0x00, 0x00, 0x12, 0x61, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
  // io::println(fmt!("result :: %?", result));

  io::println("++++++++++++++++++++++++++++++++++++++++++++++");
  match result {
    @Document(map) => io::println(fmt!("got document :: %?", map)),
    @ParseError(result) => io::println(fmt!("got error :: %?", result))
  }
  // assert_eq!(false, result.is_error());
  // Convert to a document
  // let document = result as @Document;
}
