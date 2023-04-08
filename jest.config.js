/** @type {import('ts-jest').JestConfigWithTsJest} */
const { join } = require("path");

const { pathsToModuleNameMapper } = require("ts-jest");
const { compilerOptions } = require("./tsconfig.json");

module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  coverageDirectory: "<rootDir>/coverage",
  moduleFileExtensions: ["ts", "js"],

  // https://kulshekhar.github.io/ts-jest/docs/getting-started/paths-mapping/
  roots: ["<rootDir>"],
  modulePaths: [compilerOptions.baseUrl],
  moduleNameMapper: pathsToModuleNameMapper(
    compilerOptions.paths /*, { prefix: '<rootDir>/' } */
  ),

  testMatch: [join(__dirname, "src/**/*.spec.(ts|js)")],
};
