#include <Columns/ColumnArray.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationArray.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>

#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}, size{0}
{
}

DataTypeArray::DataTypeArray(const DataTypePtr & nested_, const size_t & _size)
    : nested{nested_}, size{_size}
{
}

std::string DataTypeArray::doGetName() const
{
    return "Array(" + nested->getName() + ", " + std::to_string(size) + ")";
}

MutableColumnPtr DataTypeArray::createColumn() const
{
    return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create(), size);
}


Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}

SerializationPtr DataTypeArray::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationArray>(nested->getDefaultSerialization());
}

size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}

// Abstracted away to be re-used in future synthetic sugar types built on top of Array.
// For example - Vec(10) => Array(Float32, 10) - to represent vectors of fixed-length Float32 optimized for high performance math operations.
static UInt64 getSizeArgument(const ASTPtr & argument)
{
    const auto * arg = argument->as<ASTLiteral>();
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Vec data type family requires size argument to be a non-negative integer", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    return arg->value.get<UInt64>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() == 0 || arguments->children.size() > 2)
        throw Exception("Array data type family must have at least one argument - type of elements, and optionally size", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);

    if (arguments->children.size() == 1)
    {
        return std::make_shared<DataTypeArray>(type, 0);
    }
    return std::make_shared<DataTypeArray>(type, getSizeArgument(arguments->children[1]));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("Array", create);
}

}
