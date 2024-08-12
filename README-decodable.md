# Releasing

Ensure the version is set properly in all of the pom.xml files (eg: 2.4.1-deco1)

Run `mvn clean install -DskipTests`

Make sure `~/.m2/settings.xml`  contains
```
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 http://maven.apache.org/xsd/settings-1.2.0.xsd">
     <mirrors>
          <mirror>
               <id>maven-default-http-blocker</id>
               <mirrorOf>dummy</mirrorOf>
               <name>Dummy mirror to override default blocking mirror that blocks http</name>
               <url>http://0.0.0.0/</url>
         </mirror>
    </mirrors>

     <servers>
        <server>
            <id>decodable-mvn-releases-local</id>
            <username>aws</username>
            <password>${env.CODEARTIFACT_AUTH_TOKEN}</password>
        </server>
```

...

Run `export AWS_PROFILE=prod`
Run ```
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain decodable --domain-owner 671293015970 --region us-west-2 --query authorizationToken --output text`
```


cd to `~/.m2/repository/com/ververica/flink-sql-<connector>-cdc>/<new-version>`

To deploy eg: Postgres, run:

```
mvn org.apache.maven.plugins:maven-deploy-plugin:2.4:deploy-file -DpomFile=./<pom-file>    \
-Dfile=./<jar-file>   \
-DrepositoryId=decodable-mvn-releases-local    \
-Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-releases-local/
```

You also need to ensure the `flink-cdc-connectors` pom.xml is availabe
cd to `~/.m2/repository/com/ververica/flink-cdc-connectors`

```
mvn org.apache.maven.plugins:maven-deploy-plugin:2.4:deploy-file -DpomFile=./flink-cdc-connectors-2.4.1-deco1.pom -Dfile=./flink-cdc-connectors-2.4.1-deco1.pom -DrepositoryId=decodable-mvn-releases-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-releases-local/
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc/target/flink-sql-connector-mysql-cdc-3.1.1-SNAPSHOT.jar -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-sql-connector-mysql-cdc
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-connect/flink-cdc-source-connectors/pom.xml -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-connect/flink-cdc-source-connectors/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-cdc-source-connectors
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-connect/pom.xml -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-connect/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-cdc-connect
```

```
mvn deploy:deploy-file -Dfile=pom.xml -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-cdc-parent
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/target/flink-connector-mysql-cdc-3.1.1-SNAPSHOT.jar -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-connector-mysql-cdc
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-common/target/flink-cdc-common-3.1.1-SNAPSHOT.jar -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-common/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-cdc-common
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-runtime/target/flink-cdc-runtime-3.1.1-SNAPSHOT.jar -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-runtime/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-cdc-runtime
```

```
mvn deploy:deploy-file -Dfile=flink-cdc-connect/flink-cdc-source-connectors/flink-connector-debezium/target/flink-connector-debezium-3.1.1-SNAPSHOT.jar -DrepositoryId=decodable-mvn-snapshots-local -Durl=https://decodable-671293015970.d.codeartifact.us-west-2.amazonaws.com/maven/decodable-mvn-snapshots-local/ -DpomFile=flink-cdc-connect/flink-cdc-source-connectors/flink-connector-debezium/pom.xml -Dversion=3.1.1-SNAPSHOT -DgroupId=org.apache.flink -DartifactId=flink-connector-debezium
```
