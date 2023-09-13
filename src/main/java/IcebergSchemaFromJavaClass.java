import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergSchemaFromJavaClass {
    public static Set<Class<?>> proccessedClasses = new HashSet<>();
    private static AtomicInteger fieldId = new AtomicInteger(1);
    public static Schema generateSchema(Class<?> clazz) throws Exception {
        List<Types.NestedField> fields = getNestedFieldList(clazz);
        return new Schema(fields);
    }

    private static Type convertJavaTypeToIcebergType(Class<?> type) throws Exception {
        if (proccessedClasses.contains(type)) {
            return null;
        }
        proccessedClasses.add(type);
        if (type == int.class || type == Integer.class) {
            return Types.IntegerType.get();
        } else if (type == String.class) {
            return Types.StringType.get();
        } else if (List.class.isAssignableFrom(type)) {
            return Types.ListType.ofOptional(fieldId.getAndIncrement(), Types.StructType.of(getNestedFieldList(type)));
        } else if (Map.class.isAssignableFrom(type)) {
            return Types.MapType.ofOptional(fieldId.getAndIncrement(), fieldId.getAndIncrement(), Types.StringType.get(), Types.StructType.of());
        } else {
            List<Types.NestedField> nestedFields = getNestedFieldList(type);
            return Types.StructType.of(nestedFields);
        }
    }

    private static List<Types.NestedField> getNestedFieldList(Class<?> type) throws Exception {
        List<Types.NestedField> nestedFields = new ArrayList<>();
        Method[] declaredMethods = type.getMethods();
        for (Method method : declaredMethods) {
            if (method.getName().startsWith("get") && method.getParameterCount() == 0) {
                String fieldName = method.getName().substring(1);
                fieldName = Character.toLowerCase(fieldName.charAt(0)) + fieldName.substring(1);
                Type icebergType = convertJavaTypeToIcebergType(method.getReturnType());
                if(icebergType != null) {
                    nestedFields.add(Types.NestedField.optional(fieldId.getAndIncrement(), fieldName, icebergType));
                }
            }
        }
        return nestedFields;
    }
}
