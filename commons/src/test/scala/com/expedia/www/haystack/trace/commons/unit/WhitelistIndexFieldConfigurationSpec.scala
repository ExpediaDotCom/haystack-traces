package com.expedia.www.haystack.trace.commons.unit

import com.expedia.www.haystack.trace.commons.config.entities.{WhiteListIndexFields, WhitelistIndexField, WhitelistIndexFieldConfiguration}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.scalatest.{Entry, FunSpec, Matchers}

class WhitelistIndexFieldConfigurationSpec extends FunSpec with Matchers {

  implicit val formats = DefaultFormats

  describe("whitelist field configuration") {
    it("an empty configuration should return whitelist fields as empty") {
      val config = WhitelistIndexFieldConfiguration()
      config.indexFieldMap shouldBe 'empty
      config.whitelistIndexFields shouldBe 'empty
    }

    it("a loaded configuration should return the non empty whitelist fields") {
      val whitelistField_1 = WhitelistIndexField(name = "role", `type` = "string")
      val whitelistField_2 = WhitelistIndexField(name = "errorcode", `type` = "long")

      val config = WhitelistIndexFieldConfiguration()
      val cfgJsonData = Serialization.write(WhiteListIndexFields(List(whitelistField_1, whitelistField_2)))

      // reload
      config.onReload(cfgJsonData)

      config.whitelistIndexFields should contain allOf(whitelistField_1, whitelistField_2)
      config.indexFieldMap.size() shouldBe 2
      config.indexFieldMap should contain allOf(Entry(whitelistField_1.name, whitelistField_1), Entry(whitelistField_2.name, whitelistField_2))

      val whitelistField_3 = WhitelistIndexField(name = "status", `type` = "string")
      val whitelistField_4 = WhitelistIndexField(name = "something", `type` = "long")

      val newCfgJsonData = Serialization.write(WhiteListIndexFields(List(whitelistField_4, whitelistField_1, whitelistField_3)))
      config.onReload(newCfgJsonData)

      config.whitelistIndexFields.size shouldBe 3
      config.whitelistIndexFields should contain allOf(whitelistField_1, whitelistField_3, whitelistField_4)
      config.indexFieldMap.size shouldBe 3
      config.indexFieldMap should contain allOf(Entry(whitelistField_1.name, whitelistField_1), Entry(whitelistField_3.name, whitelistField_3), Entry(whitelistField_4.name, whitelistField_4))

      config.onReload(newCfgJsonData)
      config.whitelistIndexFields.size shouldBe 3
      config.whitelistIndexFields should contain allOf(whitelistField_1, whitelistField_3, whitelistField_4)
      config.indexFieldMap.size() shouldBe 3
      config.indexFieldMap should contain allOf(Entry(whitelistField_1.name, whitelistField_1), Entry(whitelistField_3.name, whitelistField_3), Entry(whitelistField_4.name, whitelistField_4))
    }
  }
}
