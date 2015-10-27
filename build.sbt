name := "sparkDAMDS"

version := "1.0"

scalaVersion := "2.10.4"

resolvers +=
  "Spark 1.0 RC" at "https://repository.apache.org/content/repositories/orgapachespark-1143"

resolvers +=
  "local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-mllib" % "1.5.0-rc3"
)

libraryDependencies += "edu.indiana.soic.spidal" % "common" % "1.0-SNAPSHOT"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
