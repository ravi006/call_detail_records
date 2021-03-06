lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.organization",
      scalaVersion := "2.11.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "cdr_etl"
  )

mainClass in assembly := Some("com.organization.ts.CDRAnalysisMain")

// Spark configuration using sbt-spark-package
spName := "organization/com.organization.ts"
sparkVersion := "2.3.2"
sparkComponents ++= Seq("core", "sql")

libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided"
)

// https://mvnrepository.com/artifact/com.github.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"


// Assembly task will create a fat jar.
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// To run using SBT task with provided dependencies
fullClasspath in Runtime := (fullClasspath in Compile).value

// Compile will fail if code has a negative scalastyle result
(compile in Compile) := {
//  scalastyle.in(Compile).toTask("").value
  (compile in Compile).value
}
// Shade rules for google dependencies
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
