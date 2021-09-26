package com.bonc.data

/**
  * 用户场景或状态
  * INDOOR室内或静止场景, ROAD道路或运动场景
  */
object Scene extends Enumeration with Serializable {
  type Scene = Value
  val INDOOR, ROAD = Value

  /**
    * 字符串转换成Scene类型
    *
    * @param string ，输入的字符串
    * @return， 输出的Scene
    */
  def stringTo(string: String): Scene = {
    var scene: Scene = null
    if (string != null){
      if(string.trim.equals("ROAD"))        scene = Scene.ROAD
      else if(string.trim.equals("INDOOR")) scene = Scene.INDOOR
    }
    scene
  }



}
