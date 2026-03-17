// tools/decoders/decoder_factory.hpp
#pragma once
#include "decoders/decoder.hpp"
#include <memory>
#include <string>

std::unique_ptr<MessageDecoder> createDecoder(const std::string& encoding);
