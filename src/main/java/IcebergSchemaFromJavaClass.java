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

    private static Type convertJavaTypeToIcebergType(Field field) {
        Class<?> fieldType = field.getType();

        if (fieldType == int.class || fieldType == Integer.class) {
            return Types.IntegerType.get();
        } else if (fieldType == String.class) {
            return Types.StringType.get();
        } else {
            return Types.StructType.of(getNestedFieldList(fieldType));
        }
    }

    private static Type convertJavaGenericTypeToIcebergType(java.lang.reflect.Type genericType) {
        if (genericType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) genericType;
            java.lang.reflect.Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (typeArguments.length > 0) {
                return convertJavaTypeToIcebergType(typeArguments[0]);
            }
        }
        return null;
    }

    private static Type convertJavaTypeToIcebergType(java.lang.reflect.Type type) {
        if (type instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type;
            if (clazz == int.class || clazz == Integer.class) {
                return Types.IntegerType.get();
            } else if (clazz == String.class) {
                return Types.StringType.get();
            } else {
                return Types.StructType.of(getNestedFieldList(clazz));
            }
        }
        return null;
    }

    private static List<Types.NestedField> getNestedFieldList(Class<?> clazz) {
        if (processedClasses.contains(clazz)) {
            return Collections.emptyList(); // Avoid infinite recursion
        }

        List<Types.NestedField> nestedFields = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        processedClasses.add(clazz);

        for (Field field : fields) {
            String fieldName = field.getName();
            Type icebergType = null;

            if (List.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType())) {
                icebergType = convertJavaGenericTypeToIcebergType(field.getGenericType());
            } else {
                icebergType = convertJavaTypeToIcebergType(field.getGenericType());
            }

            if (icebergType != null) {
                nestedFields.add(Types.NestedField.optional(fieldId.getAndIncrement(), fieldName, icebergType));
            }
        }

        return nestedFields;
    }

}
