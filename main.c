#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <assert.h>

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <msgpack.h>

#include <unistd.h>

#define CHANNEL 1

void die_on_error(int x, char const *context)
{
  if (x < 0) {
    fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x));
    exit(1);
  }
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
  switch (x.reply_type) {
  case AMQP_RESPONSE_NORMAL:
    return;

  case AMQP_RESPONSE_NONE:
    fprintf(stderr, "%s: missing RPC reply type!\n", context);
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (x.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
      fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
      fprintf(stderr, "%s: server channel error %uh, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    default:
      fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
      break;
    }
    break;
  }

  exit(1);
}

size_t msgpack_buff_len;

void msgpack_pack_str_(msgpack_packer *pk, const char *str)
{
 msgpack_buff_len = strlen(str);
 msgpack_pack_str(pk, msgpack_buff_len);
 msgpack_pack_str_body(pk, str, msgpack_buff_len);
}

msgpack_sbuffer* getTestMsgPackData()
{
    // пакуем данные в буфер, содержащийся в памяти. Без сжатия (для сжатия с использование zlib используется другой тип буфера)
    msgpack_sbuffer *sbuf = malloc(sizeof(msgpack_sbuffer));
    msgpack_sbuffer_init(sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, sbuf, msgpack_sbuffer_write);

    const char *fw_id = "12345678";

    //msgpack_pack_str(&pk, strlen(fw_id));
    //msgpack_pack_str_body(&pk, fw_id, strlen(fw_id));
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str_(&pk, fw_id);
    msgpack_pack_true(&pk);
    //msgpack_pack_map(&pk, 2);
    msgpack_pack_int(&pk, 10);
    msgpack_pack_int(&pk, 10);

    // msgpack_sbuffer_destroy(&sbuf);

    return sbuf;
}

int main(int argc, char const *const *argv)
{
  char const *hostname = "localhost";
  int port = 5672;
  amqp_bytes_t exchange    = amqp_cstring_bytes("");
  amqp_bytes_t queue  = amqp_cstring_bytes("fw_queue_mq");
  char const *messagebody = "hello world";

  // Выделяет память и инициализирует новый объект типа amqp_connection_state_t
  amqp_connection_state_t conn = amqp_new_connection();
  assert(conn != NULL);
  //if (!conn) {
  //  die("Error alloc new connection");
  //}
  
  // Создаем новый tcp сокет
  amqp_socket_t* socket = amqp_tcp_socket_new(conn);
  assert(socket != NULL);
  //if (!socket) {
  //  die("Error opening tcp socket");
  //}

  // Открывает соединение по сокету
  // amqp_socket_open_noblock - неблокирующее открытие соединения (по таймауту)
  // amqp_status_enum
  int status = amqp_socket_open(socket, hostname, port);
  assert(status == AMQP_STATUS_OK);
  //if (status != AMQP_STATUS_OK) {
  //  die("Error opening SSL/TLS connection");
  //}

  // выполняет процедуру логина в брокере
  amqp_rpc_reply_t rpc_reply = amqp_login(conn,
										  "/",
										  AMQP_DEFAULT_MAX_CHANNELS,
										  AMQP_DEFAULT_FRAME_SIZE,
										  0, // отключает поддержку heartbeat
										  AMQP_SASL_METHOD_PLAIN,
										  "guest",
										  "guest");
  assert(rpc_reply.reply_type == AMQP_RESPONSE_NORMAL);
  // amqp_login заворачивается в die_on_amqp_error

  // Открывает канал для общения с брокером (разные каналы открываются в многопоточных программах)
  amqp_channel_open(conn, CHANNEL);
  // TODO: die_on_amqp_error
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  assert(reply.reply_type == AMQP_RESPONSE_NORMAL);

  // обьявляем очередь
  amqp_queue_declare(conn,
                    CHANNEL,
                    queue, // amqp_empty_bytes - для предоставления имени очереди сгенерированную динамически
                    0, // passive - если установлен, то сервер возвращает ошибку если очередь не существует
                    1, // durable - очередь сохраняется на диске
                    // https://stackoverflow.com/questions/21248563/rabbitmq-difference-between-exclusive-and-auto-delete
                    0, // exclusive - автоматически удалять очередь когда текущий коннекшн закрывается
                    0, // auto_delete - удаляет exchange, когда все очереди заканчивают её использование
                    amqp_empty_table);
   die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

   // обьявляем канал транзакционным
   amqp_tx_select(conn, CHANNEL);
   die_on_amqp_error(amqp_get_rpc_reply(conn), "Channel on transaction");

  // отправляем сообщение в очередь
  amqp_basic_properties_t props;
  // props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
  //props.content_type = amqp_cstring_bytes("application/msgpack");
  props.delivery_mode = 2;
  
//!START_MSGPACK
   msgpack_sbuffer *sbuf = getTestMsgPackData();
   amqp_bytes_t data;
   data.bytes = sbuf->data;
   data.len = sbuf->size;
//!END_MSGPACK

  printf("%s", data.bytes);
  exit(1);

  int ret = amqp_basic_publish(conn,
                     CHANNEL,
                     exchange,
                     queue, // т.к. тип exchange amq.direct, то в качестве routing_key используем имя очереди
                     1,
                     0,
                     &props,
                     data);
  die_on_error(ret, "Error");

//!START_MSGPACK
  msgpack_sbuffer_destroy(sbuf);
//!END_MSGPACK

  // проверяем транзакцию
  // sleep(10);

  // коммитим изменения
  amqp_tx_commit(conn, CHANNEL);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Confirm transaction");

  amqp_channel_close(conn, CHANNEL, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);
 
  return 0;
}
