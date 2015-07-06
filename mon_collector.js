// Демон мониторинга
// Получает из базы список устройств
// Периодически их опрашивает по HTTP
// Получает info — информация об устройстве
// Получает sensors — информация с датчиков

var process = require('process');
var log4js = require('log4js');
var rpc = require('json-rpc2');
var influxDB = require('influx');
var pg = require('pg');

var config = require('./config.json');
var influx = influxDB(config.influxDB);
var log = log4js.getLogger();

var devices = [];
var saveSensors = [];
var saveTime = new Date();

// Connect to database
pg.connect(config.database, function(err, PGClient, done) {
  if (err) {
    log.error('Error connectiong to PostgeSQL', err.message);
    process.exit(1);
  }
  getDevices(PGClient);
});

var Mock = {};
Mock.query = function(q, callback) {
  res = [{ip: '127.0.0.1'}];
  callback(false, res);
}

// Запрос списка устройств из postgreSQL
function getDevices(PGClient) {
  Mock.query('SELECT * from devices.get_devices()', function(err, result) {
    if (err) {
      log.error(err);
      process.exit(1);
    }
    devices = result;
    log.info('DEVICES LIST', devices);

    devices.forEach(function(device) {
      device.client = rpc.Client.$create(config.agent.port, device.ip);
    });
    
    getDeviceInfo();
  });
}

/*
// Обновление информации об устройствах раз в 5 минут
setInterval(getDeviceInfo, config.intervalInfo);

// Опрос серверов раз в 1 минуту
setInterval(getSensors, config.intervalSensors);
*/

function seriesName(device, sensor) {
  var s = config.seriesName;
  s = s.replace('{{device}}', device);
  s = s.replace('{{sensor}}', sensor);
  return s; 
}

function seriesNameRate(device, sensor) {
  var s = config.seriesNameRate;
  s = s.replace('{{device}}', device);
  s = s.replace('{{sensor}}', sensor);
  return s; 
}

function getDeviceInfo() {
  if (devices.length) {
    devices.forEach(function(device) {
      device.client.call('getDeviceInfo', {}, function(err, res) {
        if (err) {
          return log.error('getDeviceInfo', err);
        }
        log.info('getDeviceInfo', res);
        device.info = res;
      });
    });
  }
}

function getSensors() {
  if (devices.length) {
    devices.forEach(function(device) {
      device.client.call('getSensors', {}, function(err, sensors) {
        if (err) {
          return log.error('getSensors', err);
        }
        
        var timeDelta = new Date() - saveTime;
        saveTime = new Date();

        sensors.forEach(function(sensor, index) {
          
          // Save point to influxDB
          var series = seriesNameRate(device.ip, sensor.name);

          influx.writePoint(series, sensor.value, function(err) {
            if (err) {
              throw err; 
            }
          });

          // Save rate to influxDB
          if (sensor.saveRate || saveSensors.length > index && timeDelta) {
            series = seriesName(device.ip, sensor.name, 'rate');
            var rate = (sensor.value - saveSensors[index].value) / timeDelta;

            if (sensor.positive && rate < 0) {
              rate = 0;
            }
            
            influx.writePoint(series, sensor.value, function(err) {
              if (err) {
                throw err; 
              }
            });
          }
        });

        saveSensors = sensors;
      });
    });
  }
}
