# MultiCloud Rewriter with Example
## SETUP
### Database setup
This project uses 3 MySQL databases on 1 server, emulating using different cloud providers. 1 database represents original table. 2 derivered databases are vertically divided by some complex rules outside of Calcite (so outside the scope of this example). 

Before starting, you have to install MySQL server on your machine, create 3 databases on that server, called `mc_db`, `mc_db_amazon` and `mc_db_google` respectively. 
```sql
CREATE SCHEMA `mc_db` ;
CREATE SCHEMA `mc_db_amazon` ;
CREATE SCHEMA `mc_db_google` ;
```

Then you have to create 3 tables, as shown below.
```sql
CREATE TABLE `mc_db`.`employees` (
  `id` int(11) NOT NULL,
  `first` varchar(255) DEFAULT NULL,
  `last` varchar(255) DEFAULT NULL
  `age` int(3) NOT NULL,
);

CREATE TABLE `mc_db_amazon`.`employees` (
  `multiid` int(11) NOT NULL,
  `id` int(11) NOT NULL,
  `age` int(3) NOT NULL,
);

CREATE TABLE `mc_db_google`.`employees` (
  `multiid` int(11) NOT NULL,
  `id` int(11) NOT NULL,
  `first` varchar(255) DEFAULT NULL,
  `last` varchar(255) DEFAULT NULL
);
```

#### Notes
![#ff0000](https://placehold.it/12/ff0000?text=+) Be sure to change database credentials in [model.json] to correct values for your MySQL server and databases.

### Application setup
This project was developed using an IntelliJ IDEA editor and is using an Apache Maven for dependency management.

Pull the project to your disk locally and open it as a Maven project in the editor of your choice.

Maven should pull all the dependencies automatically if you're using IntelliJ IDEA. If that's the case, you're ready to go. `Application` module has `main()` method in it, just run it.

## Additional information
### How it works?
`Core` project uses [Apache Calcite]. More details coming soon.

TODO: Add more details.

# Disclaimer
This CloudSec project has been funded with support from the European Union.

![alt text][EU logo]

[model.json]: application/src/main/resources/model.json
[EU logo]: http://europski-fondovi.eu/sites/default/files/logo-slike/ERDF.png "European Regional Development Fund"
[Apache Calcite]: https://calcite.apache.org "Apache Calcite official website"
