#include <HalonMTA.h>
#include <syslog.h>
#include <librdkafka/rdkafka.h>
#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <stdexcept>
#include <cstring>

static std::atomic<bool> running(true);

struct Kafka
{
	rd_kafka_t *rk;
	std::thread poll;
};

static std::map<std::string, Kafka> kafkaConfig;

HALON_EXPORT
int Halon_version()
{
	return HALONMTA_PLUGIN_VERSION;
}

HALON_EXPORT
void kafka_producer(HalonHSLContext *hhc, HalonHSLArguments *args, HalonHSLValue *ret)
{
	HalonHSLValue *id_ = HalonMTA_hsl_argument_get(args, 0);
	if (!id_ || HalonMTA_hsl_value_type(id_) != HALONMTA_HSL_TYPE_STRING)
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid id argument", 0);
		return;
	}

	HalonHSLValue *topic_ = HalonMTA_hsl_argument_get(args, 1);
	if (!topic_ || HalonMTA_hsl_value_type(topic_) != HALONMTA_HSL_TYPE_STRING)
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid topic argument", 0);
		return;
	}

	HalonHSLValue *payload_ = HalonMTA_hsl_argument_get(args, 2);
	if (payload_ && (HalonMTA_hsl_value_type(payload_) != HALONMTA_HSL_TYPE_STRING && HalonMTA_hsl_value_type(payload_) != HALONMTA_HSL_TYPE_NONE))
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid value argument", 0);
		return;
	}

	HalonHSLValue *key_ = HalonMTA_hsl_argument_get(args, 3);
	if (key_ && (HalonMTA_hsl_value_type(key_) != HALONMTA_HSL_TYPE_STRING && HalonMTA_hsl_value_type(key_) != HALONMTA_HSL_TYPE_NONE))
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid key argument", 0);
		return;
	}

	HalonHSLValue *headers_ = HalonMTA_hsl_argument_get(args, 4);
	if (headers_ && HalonMTA_hsl_value_type(headers_) != HALONMTA_HSL_TYPE_ARRAY)
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid header argument", 0);
		return;
	}

	HalonHSLValue *partition_ = HalonMTA_hsl_argument_get(args, 5);
	if (partition_ && HalonMTA_hsl_value_type(partition_) != HALONMTA_HSL_TYPE_NUMBER)
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "Invalid partition argument", 0);
		return;
	}

	HalonHSLValue *block_ = HalonMTA_hsl_argument_get(args, 6);
	if (block_ && HalonMTA_hsl_value_type(block_) != HALONMTA_HSL_TYPE_BOOLEAN)
	{
		HalonHSLValue* e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "argument is not a boolean", 0);
		return;
	}

	char *id = nullptr;
	HalonMTA_hsl_value_get(id_, HALONMTA_HSL_TYPE_STRING, &id, nullptr);

	auto kafka_ = kafkaConfig.find(id);
	if (kafka_ == kafkaConfig.end())
	{
		HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
		HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, "No such id", 0);
		return;
	}

	rd_kafka_headers_t *hdrs = nullptr;
	if (headers_)
	{
		size_t index = 0;
		HalonHSLValue *k, *v;
		while ((v = HalonMTA_hsl_value_array_get(headers_, index, &k)))
		{
			if (HalonMTA_hsl_value_type(k) != HALONMTA_HSL_TYPE_STRING ||
				HalonMTA_hsl_value_type(v) != HALONMTA_HSL_TYPE_STRING)
			{
				++index;
				continue;
			}
			if (!hdrs)
				hdrs = rd_kafka_headers_new(HalonMTA_hsl_value_array_length(headers_));

			char *key = nullptr;
			size_t keylen = 0;
			HalonMTA_hsl_value_get(k, HALONMTA_HSL_TYPE_STRING, &key, &keylen);
			char *val = nullptr;
			size_t vallen = 0;
			HalonMTA_hsl_value_get(v, HALONMTA_HSL_TYPE_STRING, &val, &vallen);

			rd_kafka_resp_err_t err = rd_kafka_header_add(hdrs, key, keylen, val, vallen);
			if (err)
			{
				rd_kafka_headers_destroy(hdrs);
				HalonHSLValue *e = HalonMTA_hsl_throw(hhc);
				HalonMTA_hsl_value_set(e, HALONMTA_HSL_TYPE_EXCEPTION, rd_kafka_err2str(err), 0);
				return;
			}

			++index;
		}
	}

	char *topic = nullptr;
	HalonMTA_hsl_value_get(topic_, HALONMTA_HSL_TYPE_STRING, &topic, nullptr);
	double partition = -1;
	if (partition_)
		HalonMTA_hsl_value_get(partition_, HALONMTA_HSL_TYPE_NUMBER, &partition, nullptr);
	char *key = nullptr;
	size_t keylen = 0;
	if (key_)
		HalonMTA_hsl_value_get(key_, HALONMTA_HSL_TYPE_STRING, &key, &keylen);
	char *payload = nullptr;
	size_t payloadlen = 0;
	if (payload_)
		HalonMTA_hsl_value_get(payload_, HALONMTA_HSL_TYPE_STRING, &payload, &payloadlen);
	bool block = false;
	if (block_)
		HalonMTA_hsl_value_get(block_, HALONMTA_HSL_TYPE_BOOLEAN, &block, nullptr);

	unsigned int msgflags = RD_KAFKA_MSG_F_COPY;
	if (block_)
		msgflags |= RD_KAFKA_MSG_F_BLOCK;

	rd_kafka_resp_err_t err = rd_kafka_producev(
		kafka_->second.rk,
		RD_KAFKA_V_TOPIC(topic),
		RD_KAFKA_V_PARTITION((int32_t)partition),
		RD_KAFKA_V_KEY(key, keylen),
		RD_KAFKA_V_VALUE(payload, payloadlen),
		RD_KAFKA_V_HEADERS(hdrs),
		RD_KAFKA_V_MSGFLAGS(msgflags), RD_KAFKA_V_END);

	HalonMTA_hsl_value_set(ret, HALONMTA_HSL_TYPE_ARRAY, nullptr, 0);

	if (err)
	{
		rd_kafka_headers_destroy(hdrs);

		HalonHSLValue *k1, *v1;
		HalonMTA_hsl_value_array_add(ret, &k1, &v1);

		double i = err;
		HalonMTA_hsl_value_set(k1, HALONMTA_HSL_TYPE_STRING, "errno", 0);
		HalonMTA_hsl_value_set(v1, HALONMTA_HSL_TYPE_NUMBER, &i, 0);

		HalonMTA_hsl_value_array_add(ret, &k1, &v1);

		HalonMTA_hsl_value_set(k1, HALONMTA_HSL_TYPE_STRING, "errstr", 0);
		HalonMTA_hsl_value_set(v1, HALONMTA_HSL_TYPE_STRING, rd_kafka_err2str(err), 0);
	}

	return;
}

HALON_EXPORT
bool Halon_command_execute(HalonCommandExecuteContext *hcec, size_t argc, const char *argv[], size_t argvl[], char **out, size_t *outlen)
{
	if (argc > 1 && strcmp(argv[0], "dump") == 0)
	{
		auto h = kafkaConfig.find(argv[1]);
		if (h == kafkaConfig.end())
		{
			*out = strdup("No such id");
			return false;
		}
	
		char *buf;
		size_t len;
		FILE* fp = open_memstream(&buf, &len);
		if (!fp)
		{
			*out = strdup("open_memstream failed");
			return false;
		}
		rd_kafka_dump(fp, h->second.rk);
		fclose(fp);
		*out = buf; /*give overship to */
		return true;
	}
	*out = strdup("dump <id>");
	return false;
}

static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	// Kafka *kafka = (Kafka *)opaque;
	if (rkmessage->err)
		syslog(LOG_ERR, "kafka: message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
}

static void logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	syslog(LOG_ERR, "kafka: %s(%i): %s: %s", fac, level, rk ? rd_kafka_name(rk) : "", buf);
}

HALON_EXPORT
bool Halon_init(HalonInitContext *hic)
{
	HalonConfig *cfg = nullptr;
	HalonMTA_init_getinfo(hic, HALONMTA_INIT_CONFIG, nullptr, 0, &cfg, nullptr);
	if (!cfg)
		return false;

	char errstr[512];
	try
	{
		auto queues = HalonMTA_config_object_get(cfg, "queues");
		if (queues)
		{
			size_t l = 0;
			HalonConfig *queue;
			while ((queue = HalonMTA_config_array_get(queues, l++)))
			{
				const char *id_ = HalonMTA_config_string_get(HalonMTA_config_object_get(queue, "id"), nullptr);
				if (!id_)
					throw std::runtime_error("Missing id");

				kafkaConfig[id_] = {nullptr, std::thread()};

				HalonConfig *config = HalonMTA_config_object_get(queue, "config");
				if (!config)
					throw std::runtime_error("Missing config");

				rd_kafka_conf_t *conf = rd_kafka_conf_new();
				rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
				rd_kafka_conf_set_log_cb(conf, logger);

				size_t k = 0;
				HalonConfig *key;
				while ((key = HalonMTA_config_object_key_get(config, k++)))
				{
					const char *key_ = HalonMTA_config_string_get(key, nullptr);
					if (!key_)
						throw std::runtime_error("Missing key in config");

					const char *val = HalonMTA_config_string_get(HalonMTA_config_object_get(config, key_), nullptr);
					if (val)
					{
						if (rd_kafka_conf_set(conf, key_, val, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
						{
							rd_kafka_conf_destroy(conf);
							throw std::runtime_error(std::string(id_) + ": " + errstr);
						}
					}
				}

				rd_kafka_conf_set_opaque(conf, &(kafkaConfig.find(id_)->second));

				auto rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
				if (!rk)
				{
					rd_kafka_conf_destroy(conf);
					throw std::runtime_error(std::string(id_) + ": " + errstr);
				}

				kafkaConfig[id_].rk = rk;
				kafkaConfig[id_].poll = std::thread([rk]() {
					while (running)
						rd_kafka_poll(rk, 1000);
				});
			}
		}
	}
	catch (const std::runtime_error &e)
	{
		syslog(LOG_CRIT, "kafka: %s", e.what());
		return false;
	}

	return true;
}

HALON_EXPORT
void Halon_cleanup()
{
	running = false;
	for (auto &i : kafkaConfig)
	{
		rd_kafka_flush(i.second.rk, 60 * 1000);
		i.second.poll.join();
		size_t qlen = rd_kafka_outq_len(i.second.rk);
		if (qlen)
			syslog(LOG_CRIT, "kafka: terminated with %zu message(s) lost in queue", qlen);
		rd_kafka_destroy(i.second.rk);
	}
}

HALON_EXPORT
bool Halon_hsl_register(HalonHSLRegisterContext *ptr)
{
	HalonMTA_hsl_module_register_function(ptr, "kafka_producer", &kafka_producer);
	return true;
}
