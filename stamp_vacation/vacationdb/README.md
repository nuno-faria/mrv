Implementation of the STAMP Vacation benchmark for SQL
databases. For more information on STAMP and the Vacation benchmark see:

 * https://github.com/kozyraki/stamp/tree/master/vacation

 *  C. Cao Minh, J. Chung, C. Kozyrakis, and K. Olukotun. STAMP: Stanford 
    Transactional Applications for Multi-processing. In IISWC '08: Proceedings
    of The IEEE International Symposium on Workload Characterization,
    September 2008.
    
There parameters are compatible with the original implmentation:
```
    -n, --number
      number of queries in each transaction
      Default: 10
    -q, --queried
      % of data queried
      Default: 90
    -r, --relations
      number of rows in relations
      Default: 65536
    -c, --clients
      number of concurrent clients
      Default: 10
    -u, --user
      % of user transactions (MakeReservation)
      Default: 80
    -t, --txn
      total number of transactions to execute
      Default: 67108864
```

These are some additional parameters:
```
    -d, --database
      JDBC database url
      Default: jdbc:derby:memory:vacation;create=true
    -P, --dbpassword
      database password
    -U, --dbuser
      database user name
    -y, --retry
      retry deadlocked/conflict transactions
      Default: false
    -i, --isolation
      isolation level
      Default: REPEATABLE_READ
      Possible Values: [NONE, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE, UNKNOWN]
    -o, --output
      output CSV files to folder
    --delete
      delete data before population
      Default: false
    --no-create
      skip table creation
      Default: false
    --no-drop
      skip table population
      Default: false
    --no-populate
      skip table population
      Default: false
    --no-run
      skip running the benchmark
      Default: false
    -h, -?, --help
      display usage information
```

Results are available by JMX while the benchmark runs and
printed to the console when it ends. Debug output can be turned on with Log4j configuration.