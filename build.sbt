
//name := "FormatTestsyeahh"
version := "0.1"
scalaVersion := "2.11.12"

//lazy val printer = Task[Unit]("", Action[Unit]())

lazy val dynamoDatasource = (project in file("."))
  .settings(
  name := "DynamoDataSource",
  libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
    "software.amazon.awssdk" % "dynamodb" % "2.7.18",
    "software.amazon.awssdk" % "netty-nio-client" % "2.7.18",
    "com.google.guava" % "guava" % "23.0",
  ), 
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-target:jvm-1.8")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "com.google.common.shaded.@1").inAll
)