package io.aeroless

import org.scalatest.{FlatSpec, Matchers}

class DslSpec extends FlatSpec with Matchers {

  val aerospikeValue = AsValue.obj(
    "name" -> "Romain",
    "age" -> 27,
    "details" -> AsValue.obj(
      "city" -> "Montpellier",
      "company" -> "Tabmo"
    )
  )

  "Value" should "be read" in {
    import cats.implicits._
    import io.aeroless.parser._

    val program = (
      get("name")(readString),
      get("age")(readLong),
      get("details") {
        get("city")(readString)
      }
    ).tupled

    program.runUnsafe(aerospikeValue) shouldBe Right(("Romain", 27, "Montpellier"))
  }

  case class Details(city: String, company: String)

  case class Person(name: String, age: Int, details: Details)
  "Value" should "be decode" in {
    val tested = Decoder[Person].dsl.runUnsafe(aerospikeValue)
    info(tested.toString)
  }
}
