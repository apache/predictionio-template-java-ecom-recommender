name := "template-java-ecom-recommender"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.11.0-incubating" % "provided",
  "org.apache.spark"        %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark"        %% "spark-mllib" % "1.3.0" % "provided",
  "com.google.guava"         % "guava" % "12.0",
  "org.jblas"                % "jblas" % "1.2.4")
