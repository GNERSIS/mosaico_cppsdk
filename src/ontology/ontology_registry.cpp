#include "ontology/ontology_registry.hpp"

#include "ontology/builders/raw_builder.hpp"
#include "ontology/builders/geometry_builders.hpp"
#include "ontology/builders/compound_geometry_builders.hpp"
#include "ontology/builders/imu_builder.hpp"
#include "ontology/builders/gps_builders.hpp"
#include "ontology/builders/nav_builders.hpp"
#include "ontology/builders/sensor_builders.hpp"
#include "ontology/builders/std_type_builder.hpp"

void OntologyRegistry::add(const std::string& tag, Factory factory) {
    factories_[tag] = std::move(factory);
}

OntologyBuilder* OntologyRegistry::find(const std::string& tag) {
    auto inst_it = instances_.find(tag);
    if (inst_it != instances_.end()) {
        return inst_it->second.get();
    }

    auto fact_it = factories_.find(tag);
    if (fact_it == factories_.end()) {
        return nullptr;
    }

    auto instance = fact_it->second();
    auto* ptr = instance.get();
    instances_[tag] = std::move(instance);
    return ptr;
}

OntologyRegistry createDefaultRegistry() {
    OntologyRegistry reg;

    // Fallback
    reg.add("raw", [] { return std::make_unique<RawBuilder>(); });

    // Geometry primitives — keys match Python SDK's camel_to_snake(ClassName)
    reg.add("vector3d", [] { return std::make_unique<Vector3dBuilder>(); });
    reg.add("point3d", [] { return std::make_unique<Point3dBuilder>(); });
    reg.add("quaternion", [] { return std::make_unique<QuaternionBuilder>(); });

    // Compound geometry
    reg.add("pose", [] { return std::make_unique<PoseBuilder>(); });
    reg.add("velocity", [] { return std::make_unique<VelocityBuilder>(); });
    reg.add("acceleration", [] { return std::make_unique<AccelerationBuilder>(); });
    reg.add("transform", [] { return std::make_unique<TransformBuilder>(); });
    reg.add("force_torque", [] { return std::make_unique<ForceTorqueBuilder>(); });

    // IMU and GPS
    reg.add("imu", [] { return std::make_unique<ImuBuilder>(); });
    reg.add("gps", [] { return std::make_unique<GpsBuilder>(); });
    reg.add("gps_status", [] { return std::make_unique<GpsStatusBuilder>(); });

    // Navigation
    reg.add("motion_state", [] { return std::make_unique<MotionStateBuilder>(); });
    reg.add("frame_transform", [] { return std::make_unique<FrameTransformBuilder>(); });

    // Sensors
    reg.add("image", [] { return std::make_unique<ImageBuilder>(); });
    reg.add("compressed_image", [] { return std::make_unique<CompressedImageBuilder>(); });
    reg.add("camera_info", [] { return std::make_unique<CameraInfoBuilder>(); });
    reg.add("battery_state", [] { return std::make_unique<BatteryStateBuilder>(); });
    reg.add("robot_joint", [] { return std::make_unique<RobotJointBuilder>(); });
    reg.add("nmea_sentence", [] { return std::make_unique<NmeaSentenceBuilder>(); });
    reg.add("roi", [] { return std::make_unique<RoiBuilder>(); });

    // Std types — tags match Python SDK's camel_to_snake convention
    reg.add("string", [] { return std::make_unique<StdTypeBuilder<arrow::StringType>>("string"); });
    reg.add("boolean", [] { return std::make_unique<StdTypeBuilder<arrow::BooleanType>>("boolean"); });
    reg.add("integer8", [] { return std::make_unique<StdTypeBuilder<arrow::Int8Type>>("integer8"); });
    reg.add("integer16", [] { return std::make_unique<StdTypeBuilder<arrow::Int16Type>>("integer16"); });
    reg.add("integer32", [] { return std::make_unique<StdTypeBuilder<arrow::Int32Type>>("integer32"); });
    reg.add("integer64", [] { return std::make_unique<StdTypeBuilder<arrow::Int64Type>>("integer64"); });
    reg.add("unsigned8", [] { return std::make_unique<StdTypeBuilder<arrow::UInt8Type>>("unsigned8"); });
    reg.add("unsigned16", [] { return std::make_unique<StdTypeBuilder<arrow::UInt16Type>>("unsigned16"); });
    reg.add("unsigned32", [] { return std::make_unique<StdTypeBuilder<arrow::UInt32Type>>("unsigned32"); });
    reg.add("unsigned64", [] { return std::make_unique<StdTypeBuilder<arrow::UInt64Type>>("unsigned64"); });
    reg.add("floating32", [] { return std::make_unique<StdTypeBuilder<arrow::FloatType>>("floating32"); });
    reg.add("floating64", [] { return std::make_unique<StdTypeBuilder<arrow::DoubleType>>("floating64"); });

    return reg;
}
