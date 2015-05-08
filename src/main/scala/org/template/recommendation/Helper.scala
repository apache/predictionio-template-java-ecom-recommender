package org.template.recommendation

object Helper {
  def ofType[T]( klass: java.lang.Class[T] ) = {
    val manifest =  new Manifest[T] {
      override def erasure = klass

      override def runtimeClass: Class[_] = klass
    }

    manifest.asInstanceOf[Manifest[T]]
  }
}
