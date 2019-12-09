### Notices

The application uses Scala / Apache Spark for calculations and transformations.

Command line program term can be interpreted differently. I've delivered it as jar artifact, running as "java -jar ...".

I trust the task contract and expect valid entry data. So, probably not all possible secure checks are implemented.

I display average value as an integer, even though in fact it is calculated as a floating point.

### Running

In order to run the application just download SensorStatistics.jar artifact and run it with a directory path as a parameter. Like:
```
> java -jar SensorStatistics.jar /absolute/path/to/csv/directory
```

# Sensor Statistics Task

Create a command line program that calculates statistics from humidity sensor data.

### Background story

The sensors are in a network, and they are divided into groups. Each sensor submits its data to its group leader.
Each leader produces a daily report file for a group. The network periodically re-balances itself, so the sensors could 
change the group assignment over time, and their measurements can be reported by different leaders. The program should 
help spot sensors with highest average humidity.

## Input

- Program takes one argument: a path to directory
- Directory contains many CSV files (*.csv), each with a daily report from one group leader
- Format of the file: 1 header line + many lines with measurements
- Measurement line has sensor id and the humidity value
- Humidity value is integer in range `[0, 100]` or `NaN` (failed measurement)
- The measurements for the same sensor id can be in the different files

### Example

leader-1.csv
```
sensor-id,humidity
s1,10
s2,88
s1,NaN
```

leader-2.csv
```
sensor-id,humidity
s2,80
s3,NaN
s2,78
s1,98
```

## Expected Output

- Program prints statistics to StdOut
- It reports how many files it processed
- It reports how many measurements it processed
- It reports how many measurements failed
- For each sensor it calculates min/avg/max humidity
- `NaN` values are ignored from min/avg/max
- Sensors with only `NaN` measurements have min/avg/max as `NaN/NaN/NaN`
- Program sorts sensors by highest avg humidity (`NaN` values go last)

### Example

```
Num of processed files: 2
Num of processed measurements: 7
Num of failed measurements: 2

Sensors with highest avg humidity:

sensor-id,min,avg,max
s2,78,82,88
s1,10,54,98
s3,NaN,NaN,NaN
```

## Notes

- Single daily report file can be very large, and can exceed program memory
- You can use any Open Source library
- Program should only use memory for its internal state (no disk, no database)
- Sensible tests are welcome
