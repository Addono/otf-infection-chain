name := "sparkstreaming-infection-chain"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.apache.spark" %% "spark-graphx" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test",
)

scalacOptions += "-target:jvm-1.8"

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
