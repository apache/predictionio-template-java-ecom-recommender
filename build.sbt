name := "template-java-parallel-ecommercerecommendation"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.13.0" % "provided",
  "org.apache.spark"        %% "spark-mllib" % "2.1.1"    % "provided",
  "org.jblas"                % "jblas" % "1.2.4")
