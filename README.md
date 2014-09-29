LockMarker
==========

Dependencies
------------

* Maven
* Dropwizard

Build
-----

* To build the API Server, run ```mvn clean package``` and get the lockmarker-0.0.1.jar.
* To build the service client CLI, run ```mvn assembly:assembly``` and get build lockmarker-0.0.1-client.jar.

Run
---

#. To start the server, go to the project root directory (the directory of pom.xml) and run::

    ```java -jar target/lockmarker-0.0.1.jar server lockmarker.yml```

#. To run the client CLI, make sure the above service is up and run::

    ```java -jar target/lockmarker-0.0.1-client.jar```

#. To execute unit tests, run::

    ```mvn test```

#. To execute interation test, make sure the server up running on localhost and run::

    ```mvn integration-test``` 

