use chrono::Utc;
use futures_util::{stream::SplitStream, SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

// --- 설정 ---
const MAINNET_API_KEY: &str = "API_KEY";
const MAINNET_SECRET_KEY: &str = "SECRET_KEY";
const WEBSOCKET_API_BASE: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
const SYMBOL: &str = "DOGEUSDT";
const QUANTITY: &str = "24";

// --- 데이터 구조체 ---
type PendingRequests = Arc<RwLock<HashMap<String, Instant>>>;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct OrderResult {
    order_id: i64,
    cum_quote: String,
    executed_qty: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WsResponse {
    id: String,
    result: Option<OrderResult>,
    error: Option<Value>,
}

// 사전 계산된 HMAC 구조체
struct PrecomputedHmac {
    mac: HmacSha256,
}

type HmacSha256 = Hmac<Sha256>;

impl PrecomputedHmac {
    fn new(secret: &str) -> Self {
        Self {
            mac: HmacSha256::new_from_slice(secret.as_bytes()).unwrap(),
        }
    }

    fn sign(&self, message: &str) -> String {
        let mut mac = self.mac.clone();
        mac.update(message.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

/// 최적화된 주문 전송 함수
async fn place_order_optimized(
    write: &Arc<RwLock<futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    side: &str,
    reduce_only: bool,
    pending_requests: PendingRequests,
    hmac: &PrecomputedHmac,
) -> Result<(), Box<dyn std::error::Error>> {
    // UUID 생성 최적화 (v4 대신 더 빠른 방식 사용 가능)
    let request_id = Uuid::new_v4().to_string();
    let timestamp = Utc::now().timestamp_millis();
    
    // 문자열 사전 할당으로 재할당 방지
    let mut query_string = String::with_capacity(256);
    
    // 직접 포맷팅으로 오버헤드 감소
    use std::fmt::Write;
    write!(&mut query_string, "apiKey={}&quantity={}&side={}&symbol={}&timestamp={}&type=MARKET",
        MAINNET_API_KEY, QUANTITY, side, SYMBOL, timestamp)?;
    
    if reduce_only {
        query_string.push_str("&reduceOnly=true");
    }
    
    // 서명 생성 (사전 계산된 HMAC 사용)
    let signature = hmac.sign(&query_string);
    
    // JSON 생성 최적화
    let request = if reduce_only {
        format!(
            r#"{{"id":"{}","method":"order.place","params":{{"apiKey":"{}","symbol":"{}","side":"{}","type":"MARKET","quantity":"{}","timestamp":{},"reduceOnly":true,"signature":"{}"}}}}"#,
            request_id, MAINNET_API_KEY, SYMBOL, side, QUANTITY, timestamp, signature
        )
    } else {
        format!(
            r#"{{"id":"{}","method":"order.place","params":{{"apiKey":"{}","symbol":"{}","side":"{}","type":"MARKET","quantity":"{}","timestamp":{},"signature":"{}"}}}}"#,
            request_id, MAINNET_API_KEY, SYMBOL, side, QUANTITY, timestamp, signature
        )
    };
    
    // 시작 시간 기록
    let start_time = Instant::now();
    
    // 비동기 쓰기로 락 최소화
    {
        pending_requests.write().await.insert(request_id.clone(), start_time);
    }
    
    // 직접 WebSocket 전송 (채널 오버헤드 제거)
    {
        let mut writer = write.write().await;
        writer.send(Message::Text(request)).await?;
    }
    
    println!("📤 [{}] 주문 전송 요청 (ID: {})", side, request_id);
    
    Ok(())
}

/// 응답 처리 (최적화)
async fn handle_responses_optimized(
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending_requests: PendingRequests,
) {
    // 버퍼 사전 할당
    let mut buffer = String::with_capacity(1024);
    
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // ping 체크 최적화
                if text.len() < 50 && text.contains("ping") {
                    continue;
                }
                
                // JSON 파싱 시도
                if let Ok(resp) = serde_json::from_str::<WsResponse>(&text) {
                    // 비동기 읽기로 락 최소화
                    let network_latency_ms = {
                        let mut pending = pending_requests.write().await;
                        pending.remove(&resp.id)
                            .map(|start_time| start_time.elapsed().as_secs_f64() * 1000.0)
                            .unwrap_or(-1.0)
                    };
                    
                    if let Some(result) = resp.result {
                        // 파싱 최적화 (unwrap_or 대신 직접 처리)
                        let executed_qty = result.executed_qty.parse::<f64>().unwrap_or(0.0);
                        let cum_quote = result.cum_quote.parse::<f64>().unwrap_or(0.0);
                        let order_price = if executed_qty > 0.0 { 
                            cum_quote / executed_qty 
                        } else { 
                            0.0 
                        };
                        
                        println!(
                            "✅ [주문 성공] ID: {}, OrderID: {}. 수량: {}, 가격: {:.5} (지연: {:.3}ms)",
                            resp.id, result.order_id, QUANTITY, order_price, network_latency_ms
                        );
                    } else if let Some(error) = resp.error {
                        eprintln!(
                            "❌ [주문 실패] ID: {}. 원인: {:?} (지연: {:.3}ms)", 
                            resp.id, error, network_latency_ms
                        );
                    }
                } else {
                    println!("📥 수신 (알 수 없는 형식): {}", text);
                }
            }
            Ok(Message::Close(_)) => {
                println!("🔌 WebSocket 연결이 종료되었습니다.");
                break;
            }
            Err(e) => {
                eprintln!("🔴 WebSocket 읽기 오류: {}", e);
                break;
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Binance 메인넷 WebSocket에 연결 중...");
    
    // WebSocket 연결
    let (ws_stream, _) = connect_async(WEBSOCKET_API_BASE).await?;
    let (write, read) = ws_stream.split();
    
    println!("✅ WebSocket 연결 성공. 주문을 시작합니다.\n");
    
    // Arc<RwLock>으로 write 감싸기
    let write = Arc::new(RwLock::new(write));
    let pending_requests: PendingRequests = Arc::new(RwLock::new(HashMap::new()));
    
    // HMAC 사전 생성
    let hmac = PrecomputedHmac::new(MAINNET_SECRET_KEY);
    
    // 응답 처리 태스크
    let response_handler_pending_reqs = Arc::clone(&pending_requests);
    tokio::spawn(handle_responses_optimized(read, response_handler_pending_reqs));
    
    // 워밍업 (JIT 컴파일 최적화)
    println!("--- 시스템 워밍업 중... ---");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // --- 매수 주문 ---
    println!("\n--- 🟢 매수 주문 3회 실행 ---");
    for i in 1..=3 {
        println!("\n--- 매수 #{i} ---");
        let write_clone = Arc::clone(&write);
        let pending_clone = Arc::clone(&pending_requests);
        
        place_order_optimized(&write_clone, "BUY", false, pending_clone, &hmac).await?;
        
        // 간격 최소화 (필요시 조정)
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    // --- 매도 주문 ---
    println!("\n\n--- 🔴 Reduce-Only 매도 주문 3회 실행 ---");
    for i in 1..=3 {
        println!("\n--- 매도 #{i} ---");
        let write_clone = Arc::clone(&write);
        let pending_clone = Arc::clone(&pending_requests);
        
        place_order_optimized(&write_clone, "SELL", true, pending_clone, &hmac).await?;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    println!("\n\n모든 주문 요청이 전송되었습니다. 1초 후 프로그램을 종료합니다.");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    println!("프로그램 종료.");
    
    Ok(())
}
