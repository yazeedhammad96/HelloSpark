package yazeeh.test.examples.spark.book.ch3.datasets

case class DeviceIoTData(
                          battery_level: Long,
                          device_id: Long,
                          device_name: String,
                          ip: String,
                          cca2: String,
                          cca3: String,
                          cn: String,
                          latitude: Double,
                          longitude: Double,
                          scale: String,
                          temp: Long,
                          humidity: Long,
                          c02_level: Long,
                          lcd: String,
                          timestamp: Long
                        )