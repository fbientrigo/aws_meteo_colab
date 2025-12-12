import { useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Loader2 } from "lucide-react";

interface __ComponentName__Props {
  className?: string;
  title: string;
  onAction?: () => void;
}

export const __ComponentName__ = ({ className, title, onAction }: __ComponentName__Props) => {
  const [isLoading, setIsLoading] = useState(false);

  const handleClick = async () => {
    if (!onAction) return;
    
    setIsLoading(true);
    try {
      await onAction();
    } catch (error) {
      console.error("Error executing action:", error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className={cn("p-4 border rounded-lg bg-card text-card-foreground shadow-sm", className)}>
      <h3 className="font-semibold text-lg mb-2">{title}</h3>
      <div className="flex justify-end">
        <Button 
          onClick={handleClick} 
          disabled={isLoading}
          variant="default"
        >
          {isLoading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          {isLoading ? "Procesando..." : "Ejecutar Acción"}
        </Button>
      </div>
    </div>
  );
};
