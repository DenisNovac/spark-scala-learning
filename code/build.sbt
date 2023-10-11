ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-course"
  )

lazy val sparkVersion = "3.2.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
).map(_.cross(CrossVersion.for3Use2_13))

lazy val sparkHelpers = "0.2.2"

/* There is no TypeTag in Scala 3
 *
 * > No implicit values were found that match type reflect.runtime.universe.TypeTag[Persom].
 * https://xebia.com/blog/using-scala-3-with-spark/
 */
libraryDependencies ++= Seq(
  "io.github.vincenzobaz" %% "spark-scala3-encoders",
  "io.github.vincenzobaz" %% "spark-scala3-udf"
).map(_ % sparkHelpers)
