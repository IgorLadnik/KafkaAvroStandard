cd C:\kafka_2.12-2.3.0
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %1 --from-beginning
