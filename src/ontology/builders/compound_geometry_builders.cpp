#include "ontology/builders/compound_geometry_builders.hpp"
#include "ontology/builders/schema_types.hpp"
#include "ontology/builders/struct_append.hpp"

// ============================================================
// PoseBuilder
// Pose: {position: Point3d, orientation: Quaternion} + header + covariance + covariance_type
// Handles Pose, PoseStamped, PoseWithCovariance, PoseWithCovarianceStamped
// The CDR decoder flattens nested fields, so "pose.position.x" or "position.x"
// ============================================================
void PoseBuilder::initSchema() {
    schema_ = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("recording_timestamp_ns", arrow::int64()),
        arrow::field("position", msco::point3dType()),
        arrow::field("orientation", msco::quaternionType()),
        arrow::field("header", msco::headerType(), /*nullable=*/true),
        arrow::field("covariance", arrow::list(arrow::float64()), /*nullable=*/true),
        arrow::field("covariance_type", arrow::int16(), /*nullable=*/true),
    });
}

arrow::Status PoseBuilder::appendRow(const FieldMap& fields) {
    // Unwrap: PoseStamped has "pose.position.x", PoseWithCovariance has "pose.position.x" too
    auto prefix = resolvePrefix(fields, "position.x", {"pose.", ""});

    auto* pos_sb = structBuilder(2);
    ARROW_RETURN_NOT_OK(msco::appendPoint3d(pos_sb, fields, prefix + "position."));

    auto* ori_sb = structBuilder(3);
    ARROW_RETURN_NOT_OK(msco::appendQuaternion(ori_sb, fields, prefix + "orientation."));

    auto* hb = structBuilder(4);
    ARROW_RETURN_NOT_OK(msco::appendHeader(hb, fields));

    auto* cov_b = static_cast<arrow::ListBuilder*>(batch_builder_->GetField(5));
    auto* cov_type_b = batch_builder_->GetFieldAs<arrow::Int16Builder>(6);
    ARROW_RETURN_NOT_OK(msco::appendCovariance(cov_b, cov_type_b, fields, "covariance"));

    return arrow::Status::OK();
}

// ============================================================
// VelocityBuilder (Twist)
// Velocity: {linear: Vector3d, angular: Vector3d} + header + covariance + covariance_type
// ============================================================
void VelocityBuilder::initSchema() {
    schema_ = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("recording_timestamp_ns", arrow::int64()),
        arrow::field("linear", msco::vector3dType(), /*nullable=*/true),
        arrow::field("angular", msco::vector3dType(), /*nullable=*/true),
        arrow::field("header", msco::headerType(), /*nullable=*/true),
        arrow::field("covariance", arrow::list(arrow::float64()), /*nullable=*/true),
        arrow::field("covariance_type", arrow::int16(), /*nullable=*/true),
    });
}

arrow::Status VelocityBuilder::appendRow(const FieldMap& fields) {
    auto prefix = resolvePrefix(fields, "linear.x", {"twist.", ""});

    auto* lin_sb = structBuilder(2);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(lin_sb, fields, prefix + "linear.", "", true));

    auto* ang_sb = structBuilder(3);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(ang_sb, fields, prefix + "angular.", "", true));

    auto* hb = structBuilder(4);
    ARROW_RETURN_NOT_OK(msco::appendHeader(hb, fields));

    auto* cov_b = static_cast<arrow::ListBuilder*>(batch_builder_->GetField(5));
    auto* cov_type_b = batch_builder_->GetFieldAs<arrow::Int16Builder>(6);
    ARROW_RETURN_NOT_OK(msco::appendCovariance(cov_b, cov_type_b, fields, "covariance"));

    return arrow::Status::OK();
}

// ============================================================
// AccelerationBuilder (Accel) — same schema as Velocity
// ============================================================
void AccelerationBuilder::initSchema() {
    schema_ = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("recording_timestamp_ns", arrow::int64()),
        arrow::field("linear", msco::vector3dType(), /*nullable=*/true),
        arrow::field("angular", msco::vector3dType(), /*nullable=*/true),
        arrow::field("header", msco::headerType(), /*nullable=*/true),
        arrow::field("covariance", arrow::list(arrow::float64()), /*nullable=*/true),
        arrow::field("covariance_type", arrow::int16(), /*nullable=*/true),
    });
}

arrow::Status AccelerationBuilder::appendRow(const FieldMap& fields) {
    auto prefix = resolvePrefix(fields, "linear.x", {"accel.", ""});

    auto* lin_sb = structBuilder(2);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(lin_sb, fields, prefix + "linear.", "", true));

    auto* ang_sb = structBuilder(3);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(ang_sb, fields, prefix + "angular.", "", true));

    auto* hb = structBuilder(4);
    ARROW_RETURN_NOT_OK(msco::appendHeader(hb, fields));

    auto* cov_b = static_cast<arrow::ListBuilder*>(batch_builder_->GetField(5));
    auto* cov_type_b = batch_builder_->GetFieldAs<arrow::Int16Builder>(6);
    ARROW_RETURN_NOT_OK(msco::appendCovariance(cov_b, cov_type_b, fields, "covariance"));

    return arrow::Status::OK();
}

// ============================================================
// TransformBuilder
// Transform: {translation: Vector3d, rotation: Quaternion, target_frame_id: string}
//            + header + covariance + covariance_type
// ============================================================
void TransformBuilder::initSchema() {
    schema_ = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("recording_timestamp_ns", arrow::int64()),
        arrow::field("translation", msco::vector3dType()),
        arrow::field("rotation", msco::quaternionType()),
        arrow::field("target_frame_id", arrow::utf8(), /*nullable=*/true),
        arrow::field("header", msco::headerType(), /*nullable=*/true),
        arrow::field("covariance", arrow::list(arrow::float64()), /*nullable=*/true),
        arrow::field("covariance_type", arrow::int16(), /*nullable=*/true),
    });
}

arrow::Status TransformBuilder::appendRow(const FieldMap& fields) {
    auto prefix = resolvePrefix(fields, "translation.x", {"transform.", ""});

    auto* trans_sb = structBuilder(2);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(trans_sb, fields, prefix + "translation."));

    auto* rot_sb = structBuilder(3);
    ARROW_RETURN_NOT_OK(msco::appendQuaternion(rot_sb, fields, prefix + "rotation."));

    // target_frame_id — from child_frame_id
    auto* tfid_b = batch_builder_->GetFieldAs<arrow::StringBuilder>(4);
    auto tfid = getString(fields, "child_frame_id");
    if (tfid) ARROW_RETURN_NOT_OK(tfid_b->Append(*tfid));
    else ARROW_RETURN_NOT_OK(tfid_b->AppendNull());

    auto* hb = structBuilder(5);
    ARROW_RETURN_NOT_OK(msco::appendHeader(hb, fields));

    auto* cov_b = static_cast<arrow::ListBuilder*>(batch_builder_->GetField(6));
    auto* cov_type_b = batch_builder_->GetFieldAs<arrow::Int16Builder>(7);
    ARROW_RETURN_NOT_OK(msco::appendCovariance(cov_b, cov_type_b, fields, "covariance"));

    return arrow::Status::OK();
}

// ============================================================
// ForceTorqueBuilder (Wrench)
// ForceTorque: {force: Vector3d, torque: Vector3d} + header + covariance + covariance_type
// ============================================================
void ForceTorqueBuilder::initSchema() {
    schema_ = arrow::schema({
        arrow::field("timestamp_ns", arrow::int64()),
        arrow::field("recording_timestamp_ns", arrow::int64()),
        arrow::field("force", msco::vector3dType()),
        arrow::field("torque", msco::vector3dType()),
        arrow::field("header", msco::headerType(), /*nullable=*/true),
        arrow::field("covariance", arrow::list(arrow::float64()), /*nullable=*/true),
        arrow::field("covariance_type", arrow::int16(), /*nullable=*/true),
    });
}

arrow::Status ForceTorqueBuilder::appendRow(const FieldMap& fields) {
    auto prefix = resolvePrefix(fields, "force.x", {"wrench.", ""});

    auto* force_sb = structBuilder(2);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(force_sb, fields, prefix + "force."));

    auto* torque_sb = structBuilder(3);
    ARROW_RETURN_NOT_OK(msco::appendVector3d(torque_sb, fields, prefix + "torque."));

    auto* hb = structBuilder(4);
    ARROW_RETURN_NOT_OK(msco::appendHeader(hb, fields));

    auto* cov_b = static_cast<arrow::ListBuilder*>(batch_builder_->GetField(5));
    auto* cov_type_b = batch_builder_->GetFieldAs<arrow::Int16Builder>(6);
    ARROW_RETURN_NOT_OK(msco::appendCovariance(cov_b, cov_type_b, fields, "covariance"));

    return arrow::Status::OK();
}
