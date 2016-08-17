import _root_.sbtassembly.AssemblyPlugin.autoImport._

name := "sparkDAMDS"

version := "1.0"

scalaVersion := "2.11.7"

resolvers +=
  "Spark 1.0 RC" at "https://repository.apache.org/content/repositories/orgapachespark-1143"

resolvers +=
  "local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided"

)

libraryDependencies += "edu.indiana.soic.spidal" % "common" % "1.0"
libraryDependencies += "commons-cli" % "commons-cli" % "1.3"
libraryDependencies += "com.google.guava" % "guava" % "14.0"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)