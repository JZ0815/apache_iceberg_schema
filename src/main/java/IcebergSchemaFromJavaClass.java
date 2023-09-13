import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergSchemaFromJavaClass {
    private static AtomicInteger fieldId = new AtomicInteger(1);
    private static Set<Class<?>> processedClasses = new HashSet<>();

    public static Schema generateSchema(Class<?> clazz) {
        List<Types.NestedField> fields = getNestedFieldList(clazz);
        return new Schema(fields);
    }

    private static Type convertJavaTypeToIcebergType(Class<?> fieldType) {
        if (fieldType == int.class || fieldType == Integer.class) {
            return Types.IntegerType.get();
        } else if (fieldType == String.class) {
            return Types.StringType.get();
        } else {
            return Types.StructType.of(getNestedFieldList(fieldType));
        }
    }

    private static Type convertJavaGenericTypeToIcebergType(Class<?> fieldType, java.lang.reflect.Type genericType) {
        if (List.class.isAssignableFrom(fieldType)) {
            Type elementType = convertJavaTypeToIcebergType((Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[0]);
            return Types.ListType.ofOptional(fieldId.getAndIncrement(), elementType);
        } else if (Map.class.isAssignableFrom(fieldType)) {
            Type keyType = Types.StringType.get(); // Map key type is assumed to be a string
            Type valueType = convertJavaTypeToIcebergType((Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[1]);
            return Types.MapType.ofOptional(fieldId.getAndIncrement(), fieldId.getAndIncrement(), keyType, valueType);
        } else {
            return convertJavaTypeToIcebergType(fieldType);
        }
    }

    private static List<Types.NestedField> getNestedFieldList(Class<?> clazz) {
        if (processedClasses.contains(clazz)) {
            return Collections.emptyList(); // Avoid infinite recursion
        }
        List<Types.NestedField> nestedFields = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            Type icebergType;
            if (List.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType())) {
                icebergType = convertJavaGenericTypeToIcebergType(field.getType(), field.getGenericType());
            } else {
                icebergType = convertJavaTypeToIcebergType(field.getType());
            }
            nestedFields.add(Types.NestedField.optional(fieldId.getAndIncrement(), fieldName, icebergType));
        }
        return nestedFields;
    }

}
