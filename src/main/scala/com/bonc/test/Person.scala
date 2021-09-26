package com.bonc.test

class Person {

  var age = 0

  def age_ (newValue: Int) = {
    if (newValue > age) age = newValue
  }

}
