use std::collections::HashMap;

use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
	#[serde(rename = "t" )]
	pub transaction_id: String,
	/// Client version
	#[serde(rename = "v", skip_serializing_if = "Option::is_none")]
	pub client_version: Option<String>,
	#[serde(flatten)]
	pub message: MessageType
}

#[derive(Debug, Serialize, Deserialize)]
// #[serde(untagged)]
#[serde(tag = "y")]
pub enum MessageType {
	#[serde(rename = "q")]
	Query(Query),
	Response(Response),
	Error(Error)
}

pub type NODE_ID = [u8; 20];

/// Queries, or KRPC message dictionaries with a "y" value of "q", contain two additional keys; "q" and "a".
/// Key "q" has a string value containing the method name of the query.
/// Key "a" has a dictionary value containing named arguments to the query.
#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
	/// Method name of the query
	#[serde(rename = "q")]
	pub method_name: String,
	#[serde(rename = "a")]
	pub args: HashMap<String, String>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Queries {
	#[serde(rename = "ping")]
	Ping,
	#[serde(rename = "find_node")]
	FindNode,
	#[serde(rename = "get_peers")]
	GetPeers,
	#[serde(rename = "announce_peer")]
	AnnouncePeer
}


/// Responses, or KRPC message dictionaries with a "y" value of "r", contain one additional key "r".
/// The value of "r" is a dictionary containing named return values.
/// Response messages are sent upon successful completion of a query.
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {

}

/// Errors, or KRPC message dictionaries with a "y" value of "e", contain one additional key "e". The value of "e" is a list.
/// The first element is an integer representing the error code. The second element is a string containing the error message.
/// Errors are sent when a query cannot be fulfilled.
#[derive(Debug, Serialize, Deserialize)]
pub struct Error {
	pub r#type: ErrorType,
	pub message: String
}

#[repr(u8)]
#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorType {
	/// Generic Error
	Generic = 201,
	/// Server Error
	Server = 202,
	/// Protocol Error, such as a malformed packet, invalid arguments, or bad token
	Protocol = 203,
	/// Method Unknown
	UnknownMethod = 204
}

/// The most basic query is a ping. "q" = "ping" A ping query has a single argument, "id" the value is a 20-byte string containing the senders node ID in network byte order.
/// The appropriate response to a ping has a single key "id" containing the node ID of the responding node.
pub struct Ping {
	/// the node ID of the responding node
	pub id: NODE_ID
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_ping_message_construction() {
		let mut ping_data = HashMap::new();
		ping_data.insert("id".to_owned(), "abcdefghij0123456789".to_owned());

		let ping = Message {
			transaction_id: "aa".to_string(),
			// message_type: "q".to_string(),
			client_version: None,
			message: MessageType::Query(Query { method_name: "ping".to_owned(), args: ping_data})
		};

		let b = serde_bencode::to_string(&ping).unwrap();
		// http://bittorrent.org/beps/bep_0005.html
		assert_eq!(b, "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe");

		let j = serde_json::to_string(&ping).unwrap();

		eprintln!("{}", j);
	}

	#[test]
	fn test_ping_message_deserialize() {

		let stringified = r#"{"t":"aa","y":"q","q":"ping","a":{"id":"abcdefghij0123456789"}}"#;

		let decoded: Message = serde_json::from_str(&stringified).unwrap();

		eprintln!("{:?}", decoded);
	}
}