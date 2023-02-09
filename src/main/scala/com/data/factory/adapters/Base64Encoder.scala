package com.data.factory.adapters

import com.data.factory.exceptions.EncoderException
import com.data.factory.ports.Encoder

class Base64Encoder extends Encoder with Serializable {

  import java.util.Base64

  def encode(input: String): String =
    if (input == null || input.isEmpty()) throw  EncoderException("Input cannot be null or empty.")
    else Base64.getEncoder.encodeToString(input.getBytes())

  def decode(input: String): String =
    if (input == null || input.isEmpty()) throw EncoderException("Input cannot be empty")
    else new String(Base64.getDecoder().decode(input))
}
