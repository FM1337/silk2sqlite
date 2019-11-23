# Silk2Sqlite

  

This is a small application I built for taking the outputted data from [Silk's](https://tools.netsa.cert.org/silk/index.html) ``rwcut`` command and inserting it into a Sqlite3 database

  

## Usage
To install this application simply do:
``go install github.com/FM1337/silk2sqlite``
after that to use it, just use the following command:
``./silk2sqlite database.db rwcut-output.txt``
