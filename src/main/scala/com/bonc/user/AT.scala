package com.bonc.user

import com.bonc.network.NetworkType.NetworkType

/**
  * 终端模型描述
  */
class AT extends Serializable {
  var imsi: String = _ //IMSI，可作为终端的键值与User等关联
  var imei: String = _ //出厂号
  var number: String = _ //编号，如果是手机则是电话号码
  var networkType: NetworkType = _ //支持的移动网络类型
  var capability: NetworkType = _ //移动网络类型
}
