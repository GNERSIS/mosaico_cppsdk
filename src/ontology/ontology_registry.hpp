#pragma once

#include "ontology/ontology_builder.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

class OntologyRegistry {
 public:
    using Factory = std::function<std::unique_ptr<OntologyBuilder>()>;

    void add(const std::string& tag, Factory factory);
    OntologyBuilder* find(const std::string& tag);

 private:
    std::unordered_map<std::string, Factory> factories_;
    std::unordered_map<std::string, std::unique_ptr<OntologyBuilder>> instances_;
};

OntologyRegistry createDefaultRegistry();
