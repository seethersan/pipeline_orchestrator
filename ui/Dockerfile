
FROM node:20-alpine AS build
WORKDIR /ui
COPY . .
RUN npm install
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /ui/dist /usr/share/nginx/html
EXPOSE 80
