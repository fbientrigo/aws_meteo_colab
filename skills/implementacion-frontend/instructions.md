# Guía de Implementación Frontend

Sigue estas reglas y convenciones para mantener la consistencia del proyecto `aws_meteo_colab`.

## 1. Stack Tecnológico
- **Framework**: React 18+ (Vite)
- **Lenguaje**: TypeScript
- **Estilos**: Tailwind CSS
- **UI Kit**: Shadcn/UI (Radix Primitives)
- **Iconos**: Lucide React
- **Estado Global**: Zustand
- **API Client**: Fetch wrapper custom (`backendApi.ts`) / Supabase Client

## 2. Estructura de Directorios (src/)
- **`components/ui/`**: Componentes base de Shadcn (Botones, Inputs, etc.). **NO modificar** la lógica de estos a menos que sea estrictamente necesario.
- **`components/[feature]/`**: Componentes de negocio agrupados por funcionalidad (ej: `map/`, `auth/`).
- **`pages/`**: Vistas principales (Rutas).
- **`services/`**: Lógica de comunicación con APIs. El archivo principal es `backendApi.ts`.
- **`store/`**: Manejo de estado global. Principalmente `useAppStore.ts`.
- **`types/`**: Definiciones de tipos TypeScript compartidas.
- **`lib/utils.ts`**: Utilidades, incluye la función `cn` para clases.

## 3. Convenciones de Código

### Componentes
- Usa **Functional Components** con interfaces de Props explícitas.
- Usa la función `cn()` para mezclar clases de Tailwind.
- **Exporta** el componente como `default` si es el único en el archivo, o `named export` si es parte de una colección.

```tsx
import { cn } from "@/lib/utils";

interface MyComponentProps {
  className?: string;
  active: boolean;
}

export const MyComponent = ({ className, active }: MyComponentProps) => {
  return (
    <div className={cn("base-class", active && "active-class", className)}>
      Contenido
    </div>
  );
};
```

### Manejo de Estado (Zustand)
- El store principal está en `src/store/useAppStore.ts`.
- Si añades nuevo estado global:
    1. Define el tipo en `AppState`.
    2. Agrega el valor inicial en `create<AppState>(...)`.
    3. Crea acciones (setters) para modificar ese estado.
    4. Mantén la lógica de negocio (como llamadas API) dentro de las acciones del store si afectan a múltiples componentes.

### Conexión con Backend
- Todas las llamadas al backend deben estar encapsuladas en `src/services/backendApi.ts`.
- Usa el tipo `ApiResponse<T>` para el retorno.
- Implementa siempre el manejo de errores `try/catch`.
- Si existe, respeta la bandera `USE_MOCK` para devolver datos simulados cuando no haya backend real.

## 4. Flujo de Trabajo Típico
1.  **Definir Tipos**: Si es una nueva entidad, agrégala a `src/types/index.ts` (o archivo específico).
2.  **Servicio**: Crea/Actualiza el método en `backendApi.ts` para obtener/guardar los datos.
3.  **Store**: Actualiza `useAppStore.ts` si los datos deben persistir o ser accesibles globalmente.
4.  **Componente**: Crea la UI en `src/components/`, consumiendo los datos del store o llamando al servicio directamente si es local.
