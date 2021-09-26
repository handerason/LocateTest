package com.bonc.user

import com.bonc.network.NetworkType.NetworkType

/**
  * 用户使用的业务
  */
class Service extends Serializable {
  var networkType: NetworkType = _ //业务支持的移动网络类型
}
