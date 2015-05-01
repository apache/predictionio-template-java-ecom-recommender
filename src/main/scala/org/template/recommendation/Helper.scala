package org.template.recommendation

import io.prediction.data.storage.BiMap
import io.prediction.data.storage.DataMap

object Helper {
  def ofType[T]( klass: java.lang.Class[T] ) = {
    val manifest =  new Manifest[T] {
      override def erasure = klass

      override def runtimeClass: Class[_] = klass
    }

    manifest.asInstanceOf[Manifest[T]]
  }

  def dataMapGet[T](dataMap: DataMap, key: String, clazz: java.lang.Class[T]) = {
    val manifest = ofType(clazz)

    dataMap.get(key)(manifest)
  }

  def dataMapGetStringList(dataMap: DataMap, key: String): List[String] = {
    dataMap.get(key)(manifest[List[String]])
  }

}
