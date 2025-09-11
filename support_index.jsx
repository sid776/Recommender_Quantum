import React from "react";
import { Outlet } from "react-router-dom";
import { Box } from "@chakra-ui/react";

export default function Support() {
  return (
    <Box className="w-full h-full">
      <div className="p-4">
        <Outlet />
      </div>
    </Box>
  );
}
